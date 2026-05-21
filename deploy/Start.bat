@echo off
setlocal

set "SERVICE=VIPilotService"

sc start "%SERVICE%" >nul 2>&1
set "RC=%ERRORLEVEL%"
if "%RC%"=="1056" (
  echo [OK] %SERVICE% already running.
  exit /b 0
)
if not "%RC%"=="0" if not "%RC%"=="1056" (
  echo [ERROR] Failed to start %SERVICE%. SC code: %RC%
  sc query "%SERVICE%"
  exit /b %RC%
)

call :WaitForState RUNNING 60
if not "%ERRORLEVEL%"=="0" (
  echo [ERROR] %SERVICE% did not reach RUNNING state in time.
  sc query "%SERVICE%"
  exit /b 1
)

echo [OK] %SERVICE% started.
exit /b 0

:GetState
set "STATE=UNKNOWN"
for /f "tokens=3,4" %%A in ('sc query "%SERVICE%" ^| findstr /R /C:"STATE *:" /C:"СОСТОЯНИЕ *:"') do (
  set "STATE=%%B"
)
exit /b 0

:WaitForState
set "TARGET=%~1"
set /a "LEFT=%~2"
:WaitLoop
call :GetState
if /I "%STATE%"=="%TARGET%" (
  exit /b 0
)
if %LEFT% LEQ 0 exit /b 1
timeout /t 1 /nobreak >nul
set /a "LEFT-=1"
goto :WaitLoop

