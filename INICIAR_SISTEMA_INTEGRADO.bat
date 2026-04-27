@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

title LHN Sovereign V90 - Integrated Startup
color 0B

set "ROOT_DIR=%CD%"
set "BACKEND_DIR=%ROOT_DIR%\backend"
set "FRONTEND_DIR=%ROOT_DIR%\frontend"
set "APP_URL=http://127.0.0.1:9090"
set "BACKEND_URL=http://127.0.0.1:9002"
set "WAIT_UI_SEC=20"

echo.
echo ================================================================
echo   LHN Sovereign V90 - Integrated Startup
echo   Nexus 9001 ^| FastAPI 9002 ^| Next.js 9090
echo ================================================================
echo.

if exist "%ROOT_DIR%\.venv\Scripts\python.exe" (
  set "PYEXE=%ROOT_DIR%\.venv\Scripts\python.exe"
) else if exist "%ROOT_DIR%\venv\Scripts\python.exe" (
  set "PYEXE=%ROOT_DIR%\venv\Scripts\python.exe"
) else (
  set "PYEXE=python"
)

if /I "%PYEXE%"=="python" (
  where python >nul 2>&1
  if errorlevel 1 (
    color 0C
    echo [ERROR] Python was not found. Install Python 3.11+ or create .venv first.
    echo See INICIAR_MANUAL.txt for setup instructions.
    pause
    exit /b 1
  )
) else if not exist "%PYEXE%" (
  color 0C
  echo [ERROR] Python executable not found: %PYEXE%
  pause
  exit /b 1
)

where npm >nul 2>&1
if errorlevel 1 (
  color 0C
  echo [ERROR] npm was not found. Install Node.js 20+ before starting the frontend.
  pause
  exit /b 1
)

if not exist "%ROOT_DIR%\.env" (
  if exist "%ROOT_DIR%\.env.example" (
    echo [INFO] .env not found. Creating local .env from .env.example.
    copy "%ROOT_DIR%\.env.example" "%ROOT_DIR%\.env" >nul
  )
)

if not exist "%FRONTEND_DIR%\node_modules" (
  echo [INFO] frontend\node_modules not found. Running npm install...
  pushd "%FRONTEND_DIR%"
  call npm install
  if errorlevel 1 (
    popd
    color 0C
    echo [ERROR] npm install failed.
    pause
    exit /b 1
  )
  popd
)

echo [1/3] Starting optional Nexus sidecar on port 9001...
if exist "%BACKEND_DIR%\nexus_chat.py" (
  start "LHN Nexus 9001" cmd /k cd /d "%BACKEND_DIR%" ^&^& "%PYEXE%" nexus_chat.py
) else (
  echo [SKIP] backend\nexus_chat.py not found.
)
timeout /t 2 /nobreak >nul

echo [2/3] Starting FastAPI backend on port 9002...
start "LHN Backend 9002" cmd /k cd /d "%BACKEND_DIR%" ^&^& set TF_CPP_MIN_LOG_LEVEL=3^&^& set TF_ENABLE_ONEDNN_OPTS=0^&^& "%PYEXE%" -m uvicorn server:app --host 127.0.0.1 --port 9002
timeout /t 2 /nobreak >nul

echo [3/3] Starting Next.js frontend on port 9090...
start "LHN Frontend 9090" cmd /k cd /d "%FRONTEND_DIR%" ^&^& npm run dev

echo.
echo [OK] Services requested.
echo      Backend: %BACKEND_URL%
echo      Panel:   %APP_URL%
echo.
echo Waiting %WAIT_UI_SEC% seconds before opening the panel...
timeout /t %WAIT_UI_SEC% /nobreak >nul

set "EDGE86=%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"
set "EDGE64=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
set "CHROME=%ProgramFiles%\Google\Chrome\Application\chrome.exe"

if exist "%EDGE86%" (
  start "" "%EDGE86%" --app=%APP_URL%
) else if exist "%EDGE64%" (
  start "" "%EDGE64%" --app=%APP_URL%
) else if exist "%CHROME%" (
  start "" "%CHROME%" --app=%APP_URL%
) else (
  start "" "%APP_URL%"
)

echo.
echo Startup complete. Close the service windows to stop backend/frontend.
echo For manual startup, read INICIAR_MANUAL.txt.
echo.
pause
endlocal
