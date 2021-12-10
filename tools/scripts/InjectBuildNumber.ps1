Param(
    [parameter(Mandatory = $true)]
    [string]
    $Version
)

Function UpdateVersionInFile {
    Param ([string]$path, [string]$prefix, [string]$suffix, [string]$verNum)

    $lines = Get-Content $path -Encoding UTF8

    $new_lines = $lines | % {
        if ($_.StartsWith($prefix)) {
            return $prefix + $verNum + $suffix
        }
        else {
            return $_
        }        
    }

    Set-Content -Path $path -Value $new_lines -Encoding UTF8
}

# Nuspec is now set directly via nuget pack orchestrated by build.ps1
# UpdateVersionInFile ((Split-Path -Parent $PSCommandPath) + '\..\nupkg\Microsoft.Azure.Storage.DataMovement.nuspec') '    <version>' '</version>' 4

UpdateVersionInFile ((Split-Path -Parent $PSCommandPath) + '\..\AssemblyInfo\SharedAssemblyInfo.cs') '[assembly: AssemblyFileVersion("' '")]' $Version

UpdateVersionInFile ((Split-Path -Parent $PSCommandPath) + '\..\..\netcore\Microsoft.Azure.Storage.DataMovement\Microsoft.Azure.Storage.DataMovement.csproj') '    <Version>' '</Version>' $Version