@echo off
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Service -Name 'VIPilotService' -ErrorAction Stop"
exit /b %ERRORLEVEL%
