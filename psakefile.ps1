FormatTaskName "------- Executing Task: {0} -------"

properties {
	$SourceDir = $PSScriptRoot
	$Solution = ((Get-ChildItem -Path $SourceDir -Filter *.sln -File)[0].FullName)
	$ArtifactsDir = Join-Path $PSScriptRoot "Artifacts"
	$PDBsDir = Join-Path $ArtifactsDir "PDBs"
	$NuGetDir = Join-Path $ArtifactsDir "NuGet"
	$TestsArtifactsDir = Join-Path $ArtifactsDir "Tests"
	$LogsDir = Join-Path $ArtifactsDir "Logs"
	$LogFilePath = Join-Path $LogsDir "buildsummary.log"
	$ErrorLogFilePath = Join-Path $LogsDir "builderrors.log"
	$VersionFilePath = Join-Path $PSScriptRoot "version.txt"
	$NugetPublishUrl = "https://relativity.jfrog.io/relativity/api/nuget/nuget-local"
	$NugetApiKeyName = "UnarmedTapirsNugetApiKey"

}

Task default -Depends Compile, Test -Description "Build and run unit tests. All the steps for a local build.";

Task Restore -Description "Restores package dependencies" {
	# Why not dotnet restore?
	# https://stackoverflow.com/a/69524636/17394184
	# Some of DMLib's projects are in old format (with packages.config), which is not supported by dotnet restore
	exec { .\.nuget\NuGet.exe @("restore")
	}
}

Task Compile -Depends Restore -Description "Compile code for this repo" {
	Compile
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

Task Package -Description "Package up the build artifacts" {
	Initialize-Folder $NuGetDir -Safe

	$version = Get-Content $VersionFilePath -First 1
	& "$PSScriptRoot\tools\scripts\InjectBuildNumber.ps1" $version
	Compile

	$nuGet = CreateNuGet $version
	MoveNuGetToArtifacts $nuGet
	SavePDBs
}

Task Publish -Depends Package -Description "Publishes NuGet package to Artifactory" {
	EnsureEnvironmentVariableForPublishing
	
	$nugetApiKey = (GetEnvironmentVariable($NugetApiKeyName)).Value
	$nupkg = Get-ChildItem -Path $ArtifactsDir -Include *.nupkg -Recurse

	if ($null -eq $nupkg) {
		throw "There's no NuGet in Artifacts that can be published."
	}

	WriteLineHost "Publishing $nupkg to $NugetPublishUrl"

	exec { .\.nuget\NuGet.exe @("push", "$nupkg", 
			"-Source", "$NugetPublishUrl",
			"-ApiKey", "$nugetApiKey",
			"-Timeout", "501")
	}
}

function Compile() {
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
	EnsureEnvironmentVariablesForTests

	$TestResultsPath = Join-Path $LogsDir "{assembly}.{framework}.TestResults.xml"
	Set-Location $SourceDir\test\$testProjectName
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

function EnsureEnvironmentVariablesForTests() {
	$variableNames = @("DM_LIB_CONNECTION_STRING_SOURCE", "DM_LIB_CONNECTION_STRING_DESTINATION", "DESTINATION_ENCRYPTION_SCOPE")

	foreach ($variableName in $variableNames) {
		EnsureEnvironmentVariableExist($variableName)
	}
}

function EnsureEnvironmentVariableForPublishing() {
	EnsureEnvironmentVariableExist($NugetApiKeyName)
}

function EnsureEnvironmentVariableExist($variableName) {
	$variable = GetEnvironmentVariable($variableName)
	
	if (!$variable) {
		Write-Host ""
		throw "Missing required environment variable - $variableName. Script cannot continue."
	}
}

function GetEnvironmentVariable($variableName) {
	return (Get-ChildItem -Path Env: | Where-Object -Property Name -eq $variableName)
}

function CreateNuGet($version) {
	$buildNupkg = $BuildConfig -eq "Release" ? "BuildNupkg.cmd" : "buildDebugNupkg.cmd"
	exec { & .\tools\nupkg\$buildNupkg $version } | Out-Null

	$nupkg = (Get-ChildItem -Filter ".\*.nupkg" | Select-Object -First 1)
	if ($null -eq $nupkg) {
		throw "NuGet creation has failed."
	} 
	
	return $nupkg
}

function MoveNuGetToArtifacts($nuGet) {
	WriteLineHost "Moving NuGet file to Artefacts directory..."

	Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse | Remove-Item
	$files = Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse
	if ($files.Length -ne 0) {
		throw "Old NuGet package left. Clear it manually and then re-run a package task."
	}

	Move-Item -Path $nuGet -Destination $NuGetDir
	$nuGetPath = Get-ChildItem -Path $NuGetDir -Include *.nupkg -Recurse | Select-Object -Index 0

	WriteLineHost "NuGet placed at $nuGetPath."
}

function WriteLineHost($msg) {
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
