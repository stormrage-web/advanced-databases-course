param(
  [string]$ComposeDir = (Split-Path -Parent $MyInvocation.MyCommand.Path) + "\.."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $ComposeDir
try {
  Write-Host "Backfilling: /sql/03_backfill_events_by_category.sql"
  docker compose exec -T clickhouse sh -lc "clickhouse-client --multiquery < /sql/03_backfill_events_by_category.sql"
  if ($LASTEXITCODE -ne 0) { throw "Backfill failed" }
  Write-Host "OK"
} finally {
  Pop-Location
}


