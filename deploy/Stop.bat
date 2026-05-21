@echo off
setlocal

sc stop VIPilotService >nul 2>&1
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" (
  echo [OK] VIPilotService stop requested.
  exit /b 0
)
if "%RC%"=="1062" (
  echo [OK] VIPilotService already stopped.
  exit /b 0
)

echo [ERROR] Failed to stop VIPilotService. SC code: %RC%
sc query VIPilotService
exit /b %RC%
