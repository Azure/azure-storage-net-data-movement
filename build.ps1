##########################################################################
# This is the Psake bootstrapper script for PowerShell.
##########################################################################

<#
.SYNOPSIS
This is a Powershell script to bootstrap a Psake build.

.DESCRIPTION
This Powershell script will download NuGet if missing, restore build tools using Nuget
and execute your build tasks with the parameters you provide.

.PARAMETER TaskList
List of build tasks to execute.

.PARAMETER Configuration
The build configuration to use. Either Debug or Release. Defaults to Debug.

.LINK

#>

[CmdletBinding()]
param(
	[string[]]$taskList = @(),

	[Parameter(Mandatory=$False)]
	[ValidateSet("Debug","Release")]
	[string]$Configuration = "Debug",
	
	[Parameter(ValueFromRemainingArguments = $true)]
	[string[]]$Args = "/help"
)

Set-StrictMode -Version 2.0

[System.Net.ServicePointManager]::SecurityProtocol = 'Tls12'

Write-Host "Bootstrapping build..."
$ToolsDir = Join-Path $PSScriptRoot ".buildtools"
$ReportGenerator = Join-Path $ToolsDir "reportgenerator.exe"
Write-Host "Importing build tools from $ToolsDir..."
Import-Module -Force "$ToolsDir\BuildHelpers.psm1" -ErrorAction Stop
Write-Host "Asserting PSBuildTools module..."
Assert-Module -Name PSBuildTools -Version 0.7.0 -Path $ToolsDir
Write-Host "Asserting psake module..."
Assert-Module -Name psake -Version 4.7.4 -Path $ToolsDir
if (-not (Test-Path $ReportGenerator))
{
	Write-Host "dotnet install"
	& dotnet tool install dotnet-reportgenerator-globaltool --version 4.1.5 --tool-path $ToolsDir
	if ($LASTEXITCODE -ne 0) { throw "An error occured while restoring build tools." }
}

$Params = @{
	taskList = $TaskList
	nologo = $true
	parameters = @{
		BuildConfig = $Configuration
		ReportGenerator = $ReportGenerator
		Passthrough = $Args
	}
	Verbose = $VerbosePreference
	Debug = $DebugPreference
}

Try
{
	Write-Host "Invoking psake"
	Invoke-PSake @Params
}
Finally
{
	$ExitCode = 0
	If ($psake.build_success -eq $False)
	{
		$ExitCode = 1
	}

	Remove-Module PSake -Force -ErrorAction SilentlyContinue
	Remove-Module PSBuildTools -Force -ErrorAction SilentlyContinue
}

Exit $ExitCode
