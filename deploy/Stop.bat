@echo off
setlocal

set "SERVICE=VIPilotService"

sc stop "%SERVICE%" >nul 2>&1
set "RC=%ERRORLEVEL%"
if "%RC%"=="1062" (
  echo [OK] %SERVICE% already stopped.
  exit /b 0
)
if not "%RC%"=="0" if not "%RC%"=="1062" (
  echo [ERROR] Failed to stop %SERVICE%. SC code: %RC%
  sc query "%SERVICE%"
  exit /b %RC%
)

call :WaitForState STOPPED 60
if not "%ERRORLEVEL%"=="0" (
  echo [ERROR] %SERVICE% did not reach STOPPED state in time.
  sc query "%SERVICE%"
  exit /b 1
)

echo [OK] %SERVICE% stopped.
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

