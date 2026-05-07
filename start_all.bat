@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================
echo   koubo Start
echo ============================================

echo.
echo [1/2] Starting Flask server...
start "koubo-server" /MIN cmd /c "set HTTPS_PROXY=& set HTTP_PROXY=& python -B web_server.py"

echo [2/2] Starting ngrok tunnel...
start "koubo-tunnel" /MIN cmd /c "set HTTPS_PROXY=& set HTTP_PROXY=& ngrok http 5000 --log=stdout"

timeout /t 5 /nobreak >nul

echo.
echo Server : http://localhost:5000
echo ngrok API: http://localhost:4040
echo.

curl -s http://localhost:4040/api/tunnels 2>nul | python -c "import sys,json; d=json.load(sys.stdin); print('Public URL: ' + d['tunnels'][0]['public_url'])" 2>nul
if %errorlevel% neq 0 (
    curl -s http://localhost:40410/api/tunnels 2>nul | python -c "import sys,json; d=json.load(sys.stdin); print('Public URL: ' + d['tunnels'][0]['public_url'])" 2>nul
)

echo.
pause
