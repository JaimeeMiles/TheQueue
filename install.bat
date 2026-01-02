@echo off
REM install.bat - Install dependencies for The Queue

cd /d "%~dp0"

echo ==========================================
echo Installing The Queue Dependencies
echo ==========================================
echo.

pip install flask waitress pyodbc sqlalchemy python-dotenv

echo.
echo ==========================================
echo Installation complete!
echo ==========================================
echo.
echo Now run start.bat to launch the application.
echo.
pause
