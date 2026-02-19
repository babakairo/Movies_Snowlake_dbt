# load_env.ps1
# Loads variables from .env into the current PowerShell session
# Usage: . .\load_env.ps1   (note the leading dot — required for scope)

$envFile = Join-Path $PSScriptRoot ".env"

Get-Content $envFile |
    Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } |
    ForEach-Object {
        $name, $value = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), 'Process')
    }

Write-Host "Environment variables loaded from .env" -ForegroundColor Green
