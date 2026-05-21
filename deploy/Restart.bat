@echo off
setlocal

set "PARENT_NO_PAUSE=%VIPILOT_NO_PAUSE%"
set "VIPILOT_NO_PAUSE=1"

call "%~dp0Stop.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :FAIL

call "%~dp0Start.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :FAIL

exit /b 0

:FAIL
echo [ERROR] Restart failed. Exit code: %RC%
if /I not "%PARENT_NO_PAUSE%"=="1" (
  echo.
  pause
)
exit /b %RC%
