@echo off
setlocal

sc start VIPilotService >nul 2>&1
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" (
  echo [OK] VIPilotService started.
  exit /b 0
)
if "%RC%"=="1056" (
  echo [OK] VIPilotService already running.
  exit /b 0
)

echo [ERROR] Failed to start VIPilotService. SC code: %RC%
sc query VIPilotService
exit /b %RC%
