function Assert-Module
{
    [CmdletBinding()]
    param(
        [string]$Name,
        [System.Version]$Version,
        [string]$Path
    )

    $moduleFolder = "$Path\$Name\$Version"
    $loadedModule = Get-Module -Name $Name | Where-Object { $PSItem.Version -eq "$Version" }
    if(-not $loadedModule)
    {
        if((-not (Test-Path $moduleFolder)))
        {
            Save-Module -Name $Name -RequiredVersion $Version -Path $Path -Force -ErrorAction Stop
        }

        $modulePath = ((Get-ChildItem -Path $moduleFolder -Recurse -Filter "$Name.psd1")[0].FullName)
        Import-Module $modulePath -Global -ErrorAction Stop
    }
}