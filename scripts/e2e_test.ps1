# Prueba end-to-end del pipeline GDELT
# Uso: .\scripts\e2e_test.ps1

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

Write-Host "=== 1. Levantando servicios core ===" -ForegroundColor Cyan
docker compose up -d --build mongodb spark-master spark-worker-1 spark-worker-2 loader dashboard

Write-Host "=== 2. Esperando MongoDB ===" -ForegroundColor Cyan
$retries = 30
for ($i = 0; $i -lt $retries; $i++) {
    $health = docker inspect --format='{{.State.Health.Status}}' gdelt-mongo 2>$null
    if ($health -eq "healthy") { break }
    Start-Sleep -Seconds 2
}
if ($health -ne "healthy") { throw "MongoDB no está healthy" }

Write-Host "=== 3. Ejecutando loader (descarga GDELT) ===" -ForegroundColor Cyan
docker compose exec -T loader python loader.py --once

Write-Host "=== 4. Verificando archivos Parquet ===" -ForegroundColor Cyan
docker compose exec -T loader sh -c "find /data/parquet -name '*.parquet' | head -20"
$parquetCount = docker compose exec -T loader sh -c "find /data/parquet -name '*.parquet' | wc -l"
Write-Host "Archivos Parquet encontrados: $parquetCount"

if ([int]$parquetCount -eq 0) {
    throw "No se generaron archivos Parquet. Revisar conectividad con GDELT."
}

Write-Host "=== 5. Ejecutando análisis Spark ===" -ForegroundColor Cyan
docker compose --profile manual run --rm spark-analysis

Write-Host "=== 6. Verificando MongoDB ===" -ForegroundColor Cyan
docker compose exec -T mongodb mongosh -u admin -p gdelt2026 --authenticationDatabase admin --quiet --eval "
  const db = db.getSiblingDB('gdelt_analytics');
  const cols = [
    'conflict_heatmap','top_countries_events','tone_sources_correlation',
    'pipeline_metadata'
  ];
  cols.forEach(c => print(c + ': ' + db.getCollection(c).countDocuments()));
"

Write-Host "=== 7. Verificando dashboard API ===" -ForegroundColor Cyan
Start-Sleep -Seconds 3
try {
    $resp = Invoke-RestMethod -Uri "http://localhost:5000/api/summary" -TimeoutSec 10
    Write-Host "Total registros en dashboard: $($resp.total_records)"
    Write-Host "Última ejecución: $($resp.last_run)"
} catch {
    Write-Host "Dashboard aún no responde (puede tardar unos segundos): $_" -ForegroundColor Yellow
}

Write-Host "`n=== Prueba E2E completada ===" -ForegroundColor Green
Write-Host "Dashboard: http://localhost:5000"
