[CmdletBinding()]
param(
    [switch]$WhatIf
)

function Get-ReleaseVersion {
    $releaseNotesPath = Join-Path $PSScriptRoot "RELEASE_NOTES.md"
    
    # Parse latest version from RELEASE_NOTES.md
    $content = Get-Content $releaseNotesPath -Raw
    if ($content -match '####\s*([\d.]+[-\w]*)\s') {
        $version = $Matches[1]
        
        # Split version into prefix and suffix
        $versionParts = $version -split '-'
        return @{
            FullVersion = $version
            VersionPrefix = $versionParts[0]
            VersionSuffix = if ($versionParts.Length -gt 1) { $versionParts[1] } else { '' }
        }
    }
    
    throw "Could not find version in release notes"
}

function Update-BuildProps {
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$VersionInfo,
        [switch]$WhatIf
    )
    
    $buildPropsPath = Join-Path $PSScriptRoot "src/Directory.Build.props"
    
    # Load Directory.Build.props
    $xml = [xml](Get-Content $buildPropsPath)
    $propertyGroup = $xml.SelectSingleNode("//PropertyGroup[1]")
    
    # Update or create version elements
    @{
        'VersionPrefix' = $VersionInfo.VersionPrefix
        'VersionSuffix' = $VersionInfo.VersionSuffix
    }.GetEnumerator() | ForEach-Object {
        $element = $propertyGroup.SelectSingleNode($_.Key)
        if ($element) {
            Write-Host "Updating $($_.Key) from '$($element.InnerText)' to '$($_.Value)'"
        } else {
            Write-Host "Creating new element $($_.Key) with value '$($_.Value)'"
        }
        
        if (-not $WhatIf) {
            if ($element) {
                $element.InnerText = $_.Value
            } else {
                $newElement = $xml.CreateElement($_.Key)
                $newElement.InnerText = $_.Value
                $propertyGroup.AppendChild($newElement)
            }
        }
    }
    
    if (-not $WhatIf) {
        $xml.Save($buildPropsPath)
        Write-Host "Updated $buildPropsPath"
    }
}

# Main execution
$versionInfo = Get-ReleaseVersion
Write-Host "Found version: $($versionInfo.FullVersion)"
Write-Host "Version prefix: $($versionInfo.VersionPrefix)"
Write-Host "Version suffix: $($versionInfo.VersionSuffix)"

Update-BuildProps -VersionInfo $versionInfo -WhatIf:$WhatIf

# Output the full version string
Write-Output $versionInfo.FullVersion 