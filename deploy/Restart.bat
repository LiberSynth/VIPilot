@echo off
setlocal

call "%~dp0Stop.bat"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" exit /b %RC%

call "%~dp0Start.bat"
set "RC=%ERRORLEVEL%"
exit /b %RC%
