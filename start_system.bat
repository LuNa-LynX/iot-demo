@echo off
echo ========================================
echo Clean Room EMS - System Startup
echo ========================================
echo.

echo [1/3] Starting Main Server...
start "EMS Main Server" cmd /k "python main.py"

timeout /t 3 /nobreak >nul

echo [2/3] Starting CSV Data Streamer...
start "CSV Data Streamer" cmd /k "python csv_data_streamer.py"

timeout /t 2 /nobreak >nul

echo [3/3] Starting Cloud Backup Service...
start "Cloud Backup" cmd /k "python cloud_backup.py"

echo.
echo ========================================
echo System Started!
echo ========================================
echo.
echo Services running:
echo - Main Server: http://localhost:8000/
echo - Dashboard: http://localhost:8000/
echo - DDU: http://localhost:8000/ddu?room=Room-1
echo.
echo Press any key to exit this window...
pause >nul

