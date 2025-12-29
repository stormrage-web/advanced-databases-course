param(
  [string]$ComposeDir = (Split-Path -Parent $MyInvocation.MyCommand.Path) + "\.."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Exec-Checked {
  param(
    [Parameter(Mandatory=$true)][string]$Cmd,
    [Parameter(Mandatory=$true)][string]$ErrorMessage
  )
  & powershell -NoProfile -Command $Cmd
  if ($LASTEXITCODE -ne 0) {
    throw $ErrorMessage
  }
}

Push-Location $ComposeDir
try {
  if (!(Test-Path -Path ..\..\data\offers_clean.csv)) {
    throw "Missing file: data\\offers_clean.csv. Run: python .\\scripts\\prepare_offers.py"
  }
  if (!(Test-Path -Path ..\..\data\raw_events.csv)) {
    throw "Missing file: data\\raw_events.csv. Run: python .\\scripts\\generate_raw_events.py"
  }

  Write-Host "Loading offers (CSVWithNames) -> ozon_analytics.ecom_offers"
  docker compose exec -T clickhouse sh -lc "clickhouse-client --query 'INSERT INTO ozon_analytics.ecom_offers (offer_id, price, seller_id, category_id, vendor) FORMAT CSVWithNames' < /data/offers_clean.csv"
  if ($LASTEXITCODE -ne 0) { throw "Failed to load ozon_analytics.ecom_offers" }

  Write-Host "Loading raw events (CSVWithNames) -> ozon_analytics.raw_events"
  docker compose exec -T clickhouse sh -lc "clickhouse-client --query 'INSERT INTO ozon_analytics.raw_events (event_time, DeviceTypeName, ApplicationName, OSName, ProvinceName, ContentUnitID) FORMAT CSVWithNames' < /data/raw_events.csv"
  if ($LASTEXITCODE -ne 0) { throw "Failed to load ozon_analytics.raw_events" }

  Write-Host "OK"
} finally {
  Pop-Location
}


