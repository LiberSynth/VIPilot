@echo off
setlocal

cd /d "%~dp0.."
if not exist ".git" (
  echo [ERROR] Repository root not found: %CD%
  exit /b 1
)

echo [1/3] Stopping VIPilotService...
call "%~dp0Stop.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ERROR] Failed to stop VIPilotService.
  exit /b %RC%
)

echo [2/3] Pulling latest sources...
git pull
if errorlevel 1 (
  echo [ERROR] git pull failed. Attempting to start service back...
  call "%~dp0Start.bat" >nul 2>&1
  exit /b 1
)

echo [3/3] Starting VIPilotService...
call "%~dp0Start.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ERROR] Failed to start VIPilotService.
  exit /b %RC%
)

echo [OK] Deploy completed successfully.
exit /b 0
