@echo off
setlocal EnableDelayedExpansion

set "GIT_TERMINAL_PROMPT=0"
set "BRANCH=main"
set "ATTEMPTS=3"
set "RETRY_DELAY=5"
set "SETTLE_SEC=8"

cd /d "%~dp0.."
if not exist ".git" (
  echo [ERROR] Repository root not found: %CD%
  exit /b 1
)

for /f "delims=" %%B in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set "BRANCH=%%B"
if "%BRANCH%"=="" set "BRANCH=main"

echo Waiting %SETTLE_SEC%s for file handles to release...
timeout /t %SETTLE_SEC% /nobreak >nul

set "TRY=0"
:PullRetry
set /a TRY+=1
echo git update attempt !TRY!/%ATTEMPTS% (branch %BRANCH%)...

call :CleanupGitLocks

git -c gc.auto=0 -c maintenance.auto=false fetch --prune origin %BRANCH%
set "FETCH_RC=!ERRORLEVEL!"
if not "!FETCH_RC!"=="0" (
  if !TRY! LSS %ATTEMPTS% (
    echo [WARN] git fetch failed, retry in %RETRY_DELAY%s...
    timeout /t %RETRY_DELAY% /nobreak >nul
    goto PullRetry
  )
  echo [ERROR] git fetch failed after %ATTEMPTS% attempts. Exit code: !FETCH_RC!
  exit /b !FETCH_RC!
)

git reset --hard "origin/%BRANCH%"
set "RESET_RC=!ERRORLEVEL!"
if not "!RESET_RC!"=="0" (
  if !TRY! LSS %ATTEMPTS% (
    echo [WARN] git reset failed, retry in %RETRY_DELAY%s...
    timeout /t %RETRY_DELAY% /nobreak >nul
    goto PullRetry
  )
  echo [ERROR] git reset failed after %ATTEMPTS% attempts. Exit code: !RESET_RC!
  exit /b !RESET_RC!
)

echo [OK] Sources updated to origin/%BRANCH%.
exit /b 0

:CleanupGitLocks
if exist ".git\index.lock" del /f /q ".git\index.lock" >nul 2>&1
for %%F in (".git\objects\pack\*.lock") do del /f /q "%%F" >nul 2>&1
exit /b 0
