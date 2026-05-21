@echo off
powershell -NoProfile -ExecutionPolicy Bypass -Command "Restart-Service -Name 'VIPilotService' -ErrorAction Stop"
exit /b %ERRORLEVEL%
