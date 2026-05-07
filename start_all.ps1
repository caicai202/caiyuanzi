$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  koubo Start" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Clear proxy for ngrok
$env:HTTPS_PROXY = ""
$env:HTTP_PROXY = ""
$env:https_proxy = ""
$env:http_proxy = ""

Write-Host ""
Write-Host "[1/2] Starting Flask server..." -ForegroundColor Yellow
$serverJob = Start-Job -Name "koubo-server" -ScriptBlock {
    Set-Location $using:scriptDir
    python -B web_server.py 2>&1
}

Write-Host "[2/2] Starting ngrok tunnel..." -ForegroundColor Yellow
$ngrokJob = Start-Job -Name "koubo-tunnel" -ScriptBlock {
    $env:HTTPS_PROXY = ""
    $env:HTTP_PROXY = ""
    ngrok http 5000 --log=stdout 2>&1
}

Start-Sleep -Seconds 5

Write-Host ""
Write-Host "  Local  : http://localhost:5000" -ForegroundColor Green

# Try to get ngrok URL
try {
    $url = Invoke-RestMethod -Uri "http://localhost:4040/api/tunnels" -TimeoutSec 3
    Write-Host "  Public : $($url.tunnels[0].public_url)" -ForegroundColor Green
} catch {
    try {
        $url = Invoke-RestMethod -Uri "http://localhost:40410/api/tunnels" -TimeoutSec 3
        Write-Host "  Public : $($url.tunnels[0].public_url)" -ForegroundColor Green
    } catch {
        Write-Host "  ngrok API not ready yet, check http://localhost:4040" -ForegroundColor DarkYellow
    }
}

Write-Host ""
Write-Host "Press Ctrl+C to stop, or close this window." -ForegroundColor Gray

try {
    while ($true) {
        Start-Sleep -Seconds 10
        if ($serverJob.State -eq "Failed") {
            Write-Host "Server crashed! Check logs." -ForegroundColor Red
            Receive-Job $serverJob | Write-Host
        }
        if ($ngrokJob.State -eq "Failed") {
            Write-Host "ngrok crashed! Check logs." -ForegroundColor Red
            Receive-Job $ngrokJob | Write-Host
        }
    }
} finally {
    Stop-Job -Name "koubo-server" -ErrorAction SilentlyContinue
    Stop-Job -Name "koubo-tunnel" -ErrorAction SilentlyContinue
}
