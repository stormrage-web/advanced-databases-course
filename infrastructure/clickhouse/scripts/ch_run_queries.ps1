param(
  [string]$ComposeDir = (Split-Path -Parent $MyInvocation.MyCommand.Path) + "\.."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $ComposeDir
try {
  Write-Host "Running analytics queries from /sql/02_analytics_queries.sql"
  docker compose exec -T clickhouse sh -lc "clickhouse-client --multiquery < /sql/02_analytics_queries.sql"
  if ($LASTEXITCODE -ne 0) { throw "Analytics queries failed" }
  Write-Host "OK"
} finally {
  Pop-Location
}


