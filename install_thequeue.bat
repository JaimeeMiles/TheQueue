@echo off
REM install_thequeue.bat
REM Creates Windows service for The Queue only
REM Run as Administrator on the server
REM Requires NSSM at C:\nssm\nssm.exe

echo ==========================================
echo Installing The Queue Service
echo ==========================================
echo.

REM Check for admin rights
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This script must be run as Administrator!
    pause
    exit /b 1
)

REM Check for NSSM
if not exist "C:\nssm\nssm.exe" (
    echo ERROR: NSSM not found at C:\nssm\nssm.exe
    echo Download from https://nssm.cc/download
    pause
    exit /b 1
)

set NSSM=C:\nssm\nssm.exe
set APPS_ROOT=C:\Apps
set VENV_PYTHON=%APPS_ROOT%\venv\Scripts\python.exe
set LOGS=%APPS_ROOT%\logs

REM Create logs directory if needed
if not exist "%LOGS%" mkdir "%LOGS%"

echo Installing JDS_TheQueue...
%NSSM% install JDS_TheQueue "%VENV_PYTHON%"
%NSSM% set JDS_TheQueue AppParameters "%APPS_ROOT%\thequeue\run.py"
%NSSM% set JDS_TheQueue AppDirectory "%APPS_ROOT%\thequeue"
%NSSM% set JDS_TheQueue DisplayName "JDS TheQueue"
%NSSM% set JDS_TheQueue Description "JD Squared Shop Floor Job Queue"
%NSSM% set JDS_TheQueue Start SERVICE_AUTO_START
%NSSM% set JDS_TheQueue AppStdout "%LOGS%\thequeue.log"
%NSSM% set JDS_TheQueue AppStderr "%LOGS%\thequeue.log"
%NSSM% set JDS_TheQueue AppStdoutCreationDisposition 4
%NSSM% set JDS_TheQueue AppStderrCreationDisposition 4

echo.
echo ==========================================
echo Service installed!
echo ==========================================
echo.
echo NEXT STEPS:
echo.
echo 1. Configure service account:
echo    C:\nssm\nssm.exe edit JDS_TheQueue
echo    Go to Log on tab, set: corp.jd2.com\SVC_JDSApps
echo.
echo 2. Start the service:
echo    net start JDS_TheQueue
echo.
echo 3. Access at: http://10.20.30.12:5002
echo.
pause
