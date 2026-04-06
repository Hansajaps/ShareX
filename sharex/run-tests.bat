@echo off
REM ShareX - Comprehensive Test Script for Windows
REM Tests replication, consistency, and all endpoints

setlocal enabledelayedexpansion

echo =====================================
echo    ShareX - Automated Test Suite
echo =====================================
echo.

REM Server List
set SERVER1=http://localhost:8081
set SERVER2=http://localhost:8082
set SERVER3=http://localhost:8083

REM Check if servers are running
echo Checking if servers are running...
echo.

curl.exe -s %SERVER1%/status >nul 2>&1
if errorlevel 1 (
    echo ERROR: Server %SERVER1% is not responding
    echo Please start all servers first using: run-all-servers.bat
    pause
    exit /b 1
)
curl.exe -s %SERVER2%/status >nul 2>&1
if errorlevel 1 (
    echo ERROR: Server %SERVER2% is not responding
    echo Please start all servers first using: run-all-servers.bat
    pause
    exit /b 1
)
curl.exe -s %SERVER3%/status >nul 2>&1
if errorlevel 1 (
    echo ERROR: Server %SERVER3% is not responding
    echo Please start all servers first using: run-all-servers.bat
    pause
    exit /b 1
)

REM Find the leader dynamically
set LEADER=
set FOLLOWER1=
set FOLLOWER2=

curl.exe -s %SERVER1%/status | findstr /i "isLeader.*true" >nul
if not errorlevel 1 (
    set LEADER=%SERVER1%
    set FOLLOWER1=%SERVER2%
    set FOLLOWER2=%SERVER3%
)

curl.exe -s %SERVER2%/status | findstr /i "isLeader.*true" >nul
if not errorlevel 1 (
    set LEADER=%SERVER2%
    set FOLLOWER1=%SERVER1%
    set FOLLOWER2=%SERVER3%
)

curl.exe -s %SERVER3%/status | findstr /i "isLeader.*true" >nul
if not errorlevel 1 (
    set LEADER=%SERVER3%
    set FOLLOWER1=%SERVER1%
    set FOLLOWER2=%SERVER2%
)

if "%LEADER%"=="" (
    echo ERROR: Cannot find leader! Election might still be in progress.
    pause
    exit /b 1
)

echo OK: All servers are running!
echo Identified LEADER   : %LEADER%
echo Identified FOLLOWER1: %FOLLOWER1%
echo Identified FOLLOWER2: %FOLLOWER2%
echo.
echo =====================================
echo Starting tests...
echo =====================================
echo.

REM Test 1: Check server status
echo [TEST 1] Checking server status...
echo.
echo Leader status:
curl.exe -s %LEADER%/status
echo.
echo Follower 1 status:
curl.exe -s %FOLLOWER1%/status
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
curl.exe -s -X POST -F "file=testfile.txt" -F "data=@testfile.txt" %LEADER%/upload > _resp.tmp
set /p UPLOAD_RESP=<_resp.tmp
del /q _resp.tmp 2>nul
echo %UPLOAD_RESP%
echo %UPLOAD_RESP% | findstr /i "uploaded" >nul
if errorlevel 1 (
    echo ERROR: Upload failed - server did not confirm successful upload
    pause
    exit /b 1
)
echo OK: File uploaded to leader
echo.

REM Wait a moment for replication
ping -n 3 localhost >nul

REM Test 4: Verify replication on followers
echo [TEST 4] Verifying replication to followers...
curl.exe -s "%LEADER%/download?file=testfile.txt" > file_leader.txt
curl.exe -s "%FOLLOWER1%/download?file=testfile.txt" > file_follower1.txt
curl.exe -s "%FOLLOWER2%/download?file=testfile.txt" > file_follower2.txt

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
curl.exe -s -X POST -F "file=fail_test.txt" -F "data=@testfile.txt" %FOLLOWER1%/upload > _resp.tmp
set /p REJECT_RESP=<_resp.tmp
del /q _resp.tmp 2>nul
echo %REJECT_RESP%
echo %REJECT_RESP% | findstr /i /c:"follower" /c:"forbidden" /c:"not leader" /c:"403" >nul
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
    curl.exe -s -X POST -F "file=file_%%i.txt" -F "data=@file_%%i.txt" %LEADER%/upload >nul 2>&1
)

ping -n 3 localhost >nul

REM Verify all files replicated
set all_ok=true
for /L %%i in (1,1,3) do (
    curl.exe -s "%FOLLOWER1%/download?file=file_%%i.txt" | findstr /v "^$" >nul 2>&1
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
echo [TEST 7] Testing large file upload ^(5MB^)...
REM Create a 5MB test file using fsutil (no variable expansion issues)
fsutil file createnew large_file.bin 5242880 >nul
if not exist large_file.bin (
    echo ERROR: Could not create large test file
    goto :cleanup
)
curl.exe -s -X POST -F "file=large_file.bin" -F "data=@large_file.bin" %LEADER%/upload > large_upload_resp.txt
findstr /i "uploaded" large_upload_resp.txt >nul
if errorlevel 1 (
    echo ERROR: Large file upload failed
    type large_upload_resp.txt
) else (
    type large_upload_resp.txt
    echo OK: Large file uploaded successfully
)
del /q large_upload_resp.txt 2>nul
echo.

:cleanup
REM Cleanup
echo =====================================
echo Cleaning up test files...
echo =====================================
del /q testfile.txt file_leader.txt file_follower1.txt file_follower2.txt 2>nul
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
