@echo off
setlocal

cd /d "%~dp0.."
if not exist ".git" (
  echo [ERROR] Repository root not found: %CD%
  exit /b 1
)

echo [1/3] Stopping VIPilotService...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Stop-Service -Name 'VIPilotService' -ErrorAction Stop"
if errorlevel 1 (
  echo [ERROR] Failed to stop VIPilotService.
  exit /b 1
)

echo [2/3] Pulling latest sources...
git pull
if errorlevel 1 (
  echo [ERROR] git pull failed. Attempting to start service back...
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Service -Name 'VIPilotService' -ErrorAction SilentlyContinue"
  exit /b 1
)

echo [3/3] Starting VIPilotService...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Service -Name 'VIPilotService' -ErrorAction Stop"
if errorlevel 1 (
  echo [ERROR] Failed to start VIPilotService.
  exit /b 1
)

echo [OK] Deploy completed successfully.
exit /b 0
