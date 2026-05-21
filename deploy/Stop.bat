@echo off
powershell -NoProfile -ExecutionPolicy Bypass -Command "Stop-Service -Name 'VIPilotService' -ErrorAction Stop"
exit /b %ERRORLEVEL%
