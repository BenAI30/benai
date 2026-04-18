$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$htmlPath = Join-Path $root 'BenAI_v3 15-04.html'
$jsPath = Join-Path $root 'benai-v3-15-04-app.js'
$utf8 = New-Object System.Text.UTF8Encoding $false
$lines = [System.IO.File]::ReadAllLines($htmlPath, $utf8)
$jsLines = $lines[1772..14265]
[System.IO.File]::WriteAllLines($jsPath, $jsLines, $utf8)
$head = ($lines[0..1770] -join "`r`n")
$tail = ($lines[14267..($lines.Length - 1)] -join "`r`n")
$newHtml = $head + "`r`n" + '<script src="benai-v3-15-04-app.js"></script>' + "`r`n" + $tail
[System.IO.File]::WriteAllText($htmlPath, $newHtml, $utf8)
Write-Host "Wrote $($jsLines.Length) lines to $jsPath"
