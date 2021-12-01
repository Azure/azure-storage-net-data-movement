FormatTaskName "------- Executing Task: {0} -------"

properties {
	$SourceDir = $PSScriptRoot
	$Solution = ((Get-ChildItem -Path $SourceDir -Filter *.sln -File)[0].FullName)
	$ArtifactsDir = Join-Path $PSScriptRoot "Artifacts"
	$PDBsDir = Join-Path $ArtifactsDir "PDBs"
	$NuGetDir = Join-Path $ArtifactsDir "NuGet"
	$TestsArtifactsDir = Join-Path $ArtifactsDir "Tests"
	$CliArtifactsDir = Join-Path $ArtifactsDir "CLI"
	$LogsDir = Join-Path $ArtifactsDir "Logs"
	$LogFilePath = Join-Path $LogsDir "buildsummary.log"
	$ErrorLogFilePath = Join-Path $LogsDir "builderrors.log"
}

Task default -Depends Compile, Test -Description "Build and run unit tests. All the steps for a local build.";

Task Restore -Description "Restores package dependencies" {
    # why not dotnet restore?
    # https://stackoverflow.com/a/69524636/17394184
    # packages.config are not supported by dotnet restore (which supports new format of csproj files)
    exec { .\.nuget\NuGet.exe @("restore")
	}
}

Task Compile -Depends Restore -Description "Compile code for this repo" {
	Initialize-Folder $ArtifactsDir -Safe
	Initialize-Folder $LogsDir -Safe

	exec { dotnet @("build", $Solution,
			("/property:Configuration=$BuildConfig"),
			("/consoleloggerparameters:Summary"),
			("/nodeReuse:False"),
			("/maxcpucount"),
			("-verbosity:quiet"),
			("/nologo"),
			("/fileloggerparameters1:LogFile=`"$LogFilePath`""),
			("/fileloggerparameters2:errorsonly;LogFile=`"$ErrorLogFilePath`""))
	}
}

Task Test -Depends TestDMTestLib;

Task TestDMTestLib {
    RunTests "DMLibTest"
    RunTests "DMLibTest_NetStandard"
}

Task PublishTests -Depends PublishTestsForLinux, PublishTestsForWindows -Description "Publish test DLLs for Windows and Linux runtime"

Task PublishTestsForLinux -Description "Publish test DLLs for Linux runtime" {
	$netCoreTestProject = Get-ChildItem $SourceDir\netcore\DMLibTest\*.csproj | Select-Object -First 1
	PublishTestsNetCore "$netCoreTestProject" "Linux" "linux-x64"
}

Task PublishTestsForWindows -Description "Publish test DLLs for Windows runtime" {
	$netCoreTestProject = Get-ChildItem $SourceDir\netcore\DMLibTest\*.csproj | Select-Object -First 1
	PublishTestsNetCore "$netCoreTestProject" "Windows" "win-x64"

	$netFrameworkTestProject = Get-ChildItem $SourceDir\test\DMLibTest\*.csproj | Select-Object -First 1
	PublishTestsNetFramework "$netFrameworkTestProject" "Windows"
}

function PublishTestsNetCore($TestProject, $TargetDirectory, $TargetRuntime, $Framework = "netcoreapp2.0") {
	
	$ArtifactsDir = [IO.Path]::Combine("$TestsArtifactsDir", "$TargetDirectory", "$Framework")
	try {
		exec { dotnet @("publish", $TestProject,
				("-r:$TargetRuntime"),
				("-f:$Framework"),
				("-o:$ArtifactsDir"))
		}
	}
	catch {
		Write-Warning "Could not publish test project $TestProject ($_)"
	}
}

function PublishTestsNetFramework($TestProject, $TargetDirectory) {
	
	$ArtifactsDir = [IO.Path]::Combine("$TestsArtifactsDir", "$TargetDirectory", "NetFramework")
	try {
		exec { dotnet @("msbuild", "-property:`"Configuration=Release`"",
				"$TestProject")
		}

		New-Item -ItemType Directory -Force -Path "$ArtifactsDir" | Out-Null
		$buildOutput = Split-Path -Path "$TestProject"
		Copy-Item -Path "$buildOutput\bin\Release\*" -Destination "$ArtifactsDir"
	}
	catch {
		Write-Warning "Could not publish test project $TestProject ($_)"
	}
}

function RunTests([string] $testProjectName) {	
	EnsureRequiredEnvVariables

	$TestResultsPath = Join-Path $LogsDir "{assembly}.{framework}.TestResults.xml"
	cd $SourceDir\test\$testProjectName
	exec { & dotnet @("test", "$testProjectName.csproj",
			"--verbosity:minimal",
			"--no-build",
			"--results-directory=$TestResultsPath",
			"/consoleloggerparameters:Summary -verbosity:quiet",
			"/p:CoverletOutputFormat=cobertura",
			"/p:collectcoverage=true",
			'/p:Exclude=\"[*.TestHelpers*]*\"'
		)
	}
}

function Get-ProjectFiles {
    return dotnet sln "$Solution" list | Select-Object -Skip 2
}

# this will be used in the future when we will figure out how to safely use the certificate
Task Sign -Description "Sign all files" {
	Get-ChildItem $PSScriptRoot -recurse `
	| Where-Object { $_.Directory.FullName -notmatch "Vendor" -and $_.Directory.FullName -notmatch "packages" -and $_.Directory.FullName -notmatch "buildtools" -and $_.Directory.FullName -notmatch "obj" -and @(".dll", ".msi", ".exe") -contains $_.Extension } `
	| Select-Object -expand FullName `
	| Set-DigitalSignature -ErrorAction Stop
}

function EnsureRequiredEnvVariables() {
	$variableNames = @("DM_LIB_CONNECTION_STRING_SOURCE", "DM_LIB_CONNECTION_STRING_DESTINATION", "DESTINATION_ENCRYPTION_SCOPE")

	foreach ($variableName in $variableNames) {
		$variable = (Get-ChildItem -Path Env: | Where-Object -Property Name -eq $variableName)
		if (!$variable) {
			Write-Host ""
			throw "Missing required environment variables (DM_LIB_CONNECTION_STRING_SOURCE, DM_LIB_CONNECTION_STRING_DESTINATION, DESTINATION_ENCRYPTION_SCOPE). Tests won't run."
		}
	}
}

function FormatEnvVariable([string] $variableName, [bool] $addExportPrefix) {
	$variableValue = (Get-ChildItem -Path Env: | Where-Object -Property Name -eq $variableName).Value

	if ($addExportPrefix) {
		return "export $variableName=$variableValue"
	}
	else {
		return "$variableName=$variableValue"
	}
}

Task Clean -Description "Delete build artifacts" {
	Initialize-Folder $ArtifactsDir

	Write-Verbose "Running Clean target on $Solution"
	exec { dotnet @("msbuild", $Solution,
			("/target:Clean"),
			("/property:Configuration=$BuildConfig"),
			("/consoleloggerparameters:Summary"),
			("/nodeReuse:False"),
			("/maxcpucount"),
			("/nologo"))
	}
}

Task Rebuild -Description "Do a rebuild" {
	Initialize-Folder $ArtifactsDir

	Write-Verbose "Running Rebuild target on $Solution"
	exec { dotnet @("msbuild", $Solution,
			("/target:Rebuild"),
			("/property:Configuration=$BuildConfig"),
			("/consoleloggerparameters:Summary"),
			("/nodeReuse:False"),
			("/maxcpucount"),
			("/nologo"),
			("/fileloggerparameters1:LogFile=`"$LogFilePath`""),
			("/fileloggerparameters2:errorsonly;LogFile=`"$ErrorLogFilePath`""))
	}
}

Task Help -Alias ? -Description "Display task information" {
	WriteDocumentation
}

Task Package -Depends Compile -Description "Package up the build artifacts" {
	Initialize-Folder $NuGetDir -Safe

	if ($BuildConfig -eq "Release") {
		& .\tools\nupkg\BuildNupkg.cmd
	}
	else {
		& .\tools\nupkg\buildDebugNupkg.cmd
	}

	MoveNuGetToArtifacts
	SavePDBs
}

function MoveNuGetToArtifacts {
	Write-Line-Host "Moving NuGet file to Artifactory directory..."

	Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse | Remove-Item
	$files = Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse
	if ($files.Length -ne 0) {
		throw "Old NuGet package left. Clear it manually and then re-run a package task."
	}

	Move-Item -Path "$SourceDir\*.nupkg" -Destination $NuGetDir

	$nuGetPath = Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse | Select-Object -index 0

	Write-Line-Host "NuGet placed in $nuGetPath"
}

function Write-Line-Host($msg) {
	Write-Host
	Write-Host $msg
	Write-Host
}

function SavePDBs {
	$destinationNet = Join-Path $PDBsDir "net452"
	Initialize-Folder $destinationNet -Safe
	$sourceNet = Join-Path ".\lib\bin\" $BuildConfig
	CopyPDBs $sourceNet $destinationNet

	$destinationNetstandard = Join-Path $PDBsDir "netstandard2.0"
	Initialize-Folder $destinationNetstandard -Safe
	$sourceNetstandard = Join-Path ".\netcore\Microsoft.Azure.Storage.DataMovement\bin\" $BuildConfig | Join-Path -ChildPath "netstandard2.0"
	CopyPDBs $sourceNetstandard $destinationNetstandard
}

function CopyPDBs($source, $destination) {
	& robocopy $source $destination Microsoft.Azure.Storage.DataMovement.pdb Microsoft.Azure.Storage.DataMovement.xml /W:5 /R:5 /MT /MIR
}
