param(
  [string]$ComposeDir = (Split-Path -Parent $MyInvocation.MyCommand.Path) + "\.."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $ComposeDir
try {
  Write-Host "Waiting for ClickHouse /ping ..."
  $ok = $false
  for ($i = 0; $i -lt 60; $i++) {
    docker compose exec -T clickhouse sh -lc "wget -qO- http://127.0.0.1:8123/ping | grep -q Ok"
    if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    Start-Sleep -Seconds 1
  }
  if (-not $ok) { throw "ClickHouse is not ready (ping failed)" }

  Write-Host "Applying schema: /sql/01_schema.sql"
  docker compose exec -T clickhouse sh -lc "clickhouse-client --multiquery < /sql/01_schema.sql"
  if ($LASTEXITCODE -ne 0) { throw "Failed to apply schema" }
  Write-Host "OK"
} finally {
  Pop-Location
}


