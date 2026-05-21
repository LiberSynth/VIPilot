@echo off
setlocal

call "%~dp0Stop.bat"
if errorlevel 1 exit /b %ERRORLEVEL%

timeout /t 1 /nobreak >nul

call "%~dp0Start.bat"
exit /b %ERRORLEVEL%
