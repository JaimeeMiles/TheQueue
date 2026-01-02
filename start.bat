@echo off
REM start.bat - Start The Queue for testing
REM Run from the TheQueue directory

cd /d "%~dp0"

echo ==========================================
echo Starting The Queue (Debug Mode)
echo ==========================================
echo.
echo Web Address: http://localhost:5002
echo.
echo Press Ctrl+C to stop
echo.

python -c "from app import create_app; app = create_app(); app.run(host='0.0.0.0', port=5002, debug=True)"

echo.
echo ==========================================
echo Application stopped or crashed
echo ==========================================
pause
