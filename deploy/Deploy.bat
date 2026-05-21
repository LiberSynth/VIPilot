@echo off
setlocal

set "PARENT_NO_PAUSE=%VIPILOT_NO_PAUSE%"
set "VIPILOT_NO_PAUSE=1"

cd /d "%~dp0.."
if not exist ".git" (
  echo [ERROR] Repository root not found: %CD%
  set "RC=1"
  goto :FAIL
)

echo [1/3] Stopping VIPilotService...
call "%~dp0Stop.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ERROR] Failed to stop VIPilotService. Exit code: %RC%
  goto :FAIL
)

echo [2/3] Pulling latest sources...
git pull
if errorlevel 1 (
  echo [ERROR] git pull failed. Attempting to start service back...
  call "%~dp0Start.bat" >nul 2>&1
  set "RC=1"
  goto :FAIL
)

echo [3/3] Starting VIPilotService...
call "%~dp0Start.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ERROR] Failed to start VIPilotService. Exit code: %RC%
  goto :FAIL
)

echo [OK] Deploy completed successfully.
exit /b 0

:FAIL
if /I not "%PARENT_NO_PAUSE%"=="1" (
  echo.
  pause
)
exit /b %RC%
