@echo off
REM ShareX - Start All Servers on Windows
REM This batch file starts all 3 servers in parallel

echo =====================================
echo    ShareX - Distributed File Storage
echo              Windows Launcher
echo =====================================
echo.

REM Check if Maven is installed
where mvn >nul 2>nul
if errorlevel 1 (
    echo ERROR: Maven is not installed or not in PATH
    echo Please install Maven 3.8+ and add it to your PATH
    pause
    exit /b 1
)

REM Create storage directories
echo Creating storage directories...
if not exist storage\server1 mkdir storage\server1
if not exist storage\server2 mkdir storage\server2
if not exist storage\server3 mkdir storage\server3

REM Check if JAR file exists
if not exist "target\sharex-1.0.0.jar" (
    echo JAR file not found. Building project...
    call mvn clean package -q
    if errorlevel 1 (
        echo ERROR: Maven build failed
        pause
        exit /b 1
    )
)

REM Start the three servers
echo.
echo Starting servers...
echo.

REM Server 1 (Leader) - Port 8081
start "ShareX - Server 1 (Leader)" cmd /k java -jar "target\sharex-1.0.0.jar" server1

REM Small delay to let server1 start
timeout /t 2 /nobreak

REM Server 2 (Follower) - Port 8082
start "ShareX - Server 2 (Follower)" cmd /k java -jar "target\sharex-1.0.0.jar" server2

REM Small delay
timeout /t 2 /nobreak

REM Server 3 (Follower) - Port 8083
start "ShareX - Server 3 (Follower)" cmd /k java -jar "target\sharex-1.0.0.jar" server3

echo.
echo =====================================
echo All servers started!
echo.
echo Leader (Server 1):   http://localhost:8081
echo Follower (Server 2): http://localhost:8082
echo Follower (Server 3): http://localhost:8083
echo.
echo Check server status:
echo   curl http://localhost:8081/status
echo.
echo To stop the servers, close the command windows.
echo =====================================
echo.
pause
