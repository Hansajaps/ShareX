@echo off
REM ShareX - Comprehensive Test Script for Windows
REM Tests replication, consistency, and all endpoints

setlocal enabledelayedexpansion

echo =====================================
echo    ShareX - Automated Test Suite
echo =====================================
echo.

REM Configuration
set LEADER=http://localhost:8081
set FOLLOWER1=http://localhost:8082
set FOLLOWER2=http://localhost:8083

REM Check if servers are running
echo Checking if servers are running...
echo.

for %%S in ("%LEADER%" "%FOLLOWER1%" "%FOLLOWER2%") do (
    curl -s %%S/status >nul 2>&1
    if errorlevel 1 (
        echo ERROR: Server %%S is not responding
        echo Please start all servers first using: run-all-servers.bat
        pause
        exit /b 1
    )
)

echo OK: All servers are running!
echo.
echo =====================================
echo Starting tests...
echo =====================================
echo.

REM Test 1: Check server status
echo [TEST 1] Checking server status...
echo.
echo Leader status:
curl -s %LEADER%/status | findstr /i "serverId isLeader"
echo.
echo Follower 1 status:
curl -s %FOLLOWER1%/status | findstr /i "serverId isLeader"
echo.

REM Test 2: Create test file
echo [TEST 2] Creating test file...
echo This is a test file for ShareX replication >> testfile.txt
if exist testfile.txt (
    echo OK: Test file created
) else (
    echo ERROR: Failed to create test file
    pause
    exit /b 1
)
echo.

REM Test 3: Upload to leader
echo [TEST 3] Uploading file to leader...
curl -s -X POST -F "file=testfile.txt" -F "[email protected]" %LEADER%/upload | findstr /i "message uploaded"
if errorlevel 1 (
    echo ERROR: Upload failed
    pause
    exit /b 1
)
echo OK: File uploaded to leader
echo.

REM Test 4: Verify replication on followers
echo [TEST 4] Verifying replication to followers...
curl -s %LEADER%/download?file=testfile.txt > file_leader.txt
curl -s %FOLLOWER1%/download?file=testfile.txt > file_follower1.txt
curl -s %FOLLOWER2%/download?file=testfile.txt > file_follower2.txt

REM Compare files
fc file_leader.txt file_follower1.txt >nul 2>&1
if errorlevel 1 (
    echo ERROR: Files on leader and follower1 differ
    pause
    exit /b 1
)
echo OK: Files are identical on leader and follower1

fc file_leader.txt file_follower2.txt >nul 2>&1
if errorlevel 1 (
    echo ERROR: Files on leader and follower2 differ
    pause
    exit /b 1
)
echo OK: Files are identical on leader and follower2
echo.

REM Test 5: Test write rejection on followers
echo [TEST 5] Testing write rejection on followers...
curl -s -X POST -F "file=fail_test.txt" -F "data=should fail" %FOLLOWER1%/upload | findstr /i "forbidden error"
if errorlevel 1 (
    echo ERROR: Follower should have rejected write but didn't
    pause
    exit /b 1
)
echo OK: Follower correctly rejected write attempt
echo.

REM Test 6: Multiple file replication
echo [TEST 6] Testing multiple file replication...
for /L %%i in (1,1,3) do (
    echo Testing multiple files - file %%i
    echo Content of file %%i >> file_%%i.txt
    curl -s -X POST -F "file=file_%%i.txt" -F "[email protected]_%%i.txt" %LEADER%/upload >nul 2>&1
)

REM Verify all files replicated
set all_ok=true
for /L %%i in (1,1,3) do (
    curl -s %FOLLOWER1%/download?file=file_%%i.txt >nul 2>&1
    if errorlevel 1 (
        set all_ok=false
    )
)

if "!all_ok!"=="true" (
    echo OK: All multiple files replicated successfully
) else (
    echo ERROR: Some files failed to replicate
)
echo.

REM Test 7: Large file handling
echo [TEST 7] Testing large file upload...
REM Create a 5MB test file
powershell -Command "[System.IO.File]::WriteAllBytes('large_file.bin', @(0..255) * 20480)"
curl -s -X POST -F "file=large_file.bin" -F [email protected] "%LEADER%/upload" | findstr /i "uploaded"
if errorlevel 1 (
    echo ERROR: Large file upload failed
) else (
    echo OK: Large file uploaded successfully
)
echo.

REM Cleanup
echo =====================================
echo Cleaning up test files...
echo =====================================
del /q testfile.txt file_leader.txt file_follower1.txt file_follower2.txt
for /L %%i in (1,1,3) do (
    del /q file_%%i.txt 2>nul
)
del /q large_file.bin 2>nul

echo.
echo =====================================
echo All tests completed!
echo =====================================
echo.
pause
