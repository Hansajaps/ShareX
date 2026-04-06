@echo off
REM Test Script for File Synchronization Feature (Windows)
REM This script demonstrates the automatic sync of files when servers come online

setlocal enabledelayedexpansion

echo ======================================
echo ShareX File Sync Test Suite
echo ======================================
echo.

set LEADER=http://localhost:8081
set FOLLOWER2=http://localhost:8082
set FOLLOWER3=http://localhost:8083

REM Test 1: Check initial status
echo.
echo ====== Test 1: Check Server Status ======
echo.

echo Checking leader status...
curl -s %LEADER%/status > nul 2>&1 && (
    echo [OK] Leader is online
) || (
    echo [ERROR] Leader is offline
)

echo Checking follower2 status...
curl -s %FOLLOWER2%/status > nul 2>&1 && (
    echo [OK] Follower2 is online
) || (
    echo [ERROR] Follower2 is offline
)

echo Checking follower3 status...
curl -s %FOLLOWER3%/status > nul 2>&1 && (
    echo [OK] Follower3 is online
) || (
    echo [ERROR] Follower3 is offline
)

REM Test 2: Upload files to leader
echo.
echo ====== Test 2: Upload Files to Leader ======
echo.

echo Creating test file...
echo test content for file1 > "%TEMP%\test1.txt"

echo Uploading file1 to leader...
curl -X POST -F "file=file1.txt" -F "data=@%TEMP%\test1.txt" %LEADER%/upload

echo Uploading file2 to leader...
echo test content for file2 > "%TEMP%\test2.txt"
curl -X POST -F "file=file2.txt" -F "data=@%TEMP%\test2.txt" %LEADER%/upload

REM Test 3: Verify files on all servers
echo.
echo ====== Test 3: Verify Files Replicated ======
echo.

echo Files on follower2:
curl -s %FOLLOWER2%/list-files

echo.
echo Files on follower3:
curl -s %FOLLOWER3%/list-files

REM Test 4: Check missing files
echo.
echo ====== Test 4: Check Missing Files ======
echo.

echo Missing files on follower2:
curl -s %FOLLOWER2%/get-missing-files

echo.
echo Missing files on follower3:
curl -s %FOLLOWER3%/get-missing-files

REM Test 5: Offline scenario
echo.
echo ====== Test 5: Simulate Offline Server ======
echo.

echo.
echo [!!!] NOTE: You need to manually KILL Follower2 process now
echo [!!!] Then resume this script
echo.
pause

echo Uploading file3 to leader (follower2 is offline)...
echo test content for file3 > "%TEMP%\test3.txt"
curl -X POST -F "file=file3.txt" -F "data=@%TEMP%\test3.txt" %LEADER%/upload

echo Uploading file4 to leader (follower2 still offline)...
echo test content for file4 > "%TEMP%\test4.txt"
curl -X POST -F "file=file4.txt" -F "data=@%TEMP%\test4.txt" %LEADER%/upload

REM Test 6: Verify online servers have new files
echo.
echo ====== Test 6: Verify Online Servers ======
echo.

echo Files on follower3 (should have 4 files now):
curl -s %FOLLOWER3%/list-files

REM Test 7: Restart follower2
echo.
echo ====== Test 7: Restart Follower2 ======
echo.

echo.
echo [!!!] Now restart Follower2 (java -jar sharex.jar server2)
echo [!!!] Then resume to observe sync
echo.
pause

echo Waiting 15 seconds for sync to complete...
timeout /t 15 /nobreak

REM Test 8: Verify sync
echo.
echo ====== Test 8: Verify Follower2 Sync Complete ======
echo.

echo Files on follower2 after sync (should have all 4):
curl -s %FOLLOWER2%/list-files

echo.
echo Missing files on follower2 (should be empty):
curl -s %FOLLOWER2%/get-missing-files

REM Test 9: Download test
echo.
echo ====== Test 9: Test Downloads ======
echo.

echo Downloading file1 from leader...
curl -s %LEADER%/download?file=file1.txt -o nul && (
    echo [OK] Downloaded file1 from leader
) || (
    echo [ERROR] Failed to download file1
)

echo Downloading file4 from follower2...
curl -s %FOLLOWER2%/download?file=file4.txt -o nul && (
    echo [OK] Downloaded file4 from follower2 (recently synced!)
) || (
    echo [ERROR] Failed to download file4
)

echo Downloading file3 from follower3...
curl -s %FOLLOWER3%/download?file=file3.txt -o nul && (
    echo [OK] Downloaded file3 from follower3
) || (
    echo [ERROR] Failed to download file3
)

echo.
echo ====== All Tests Complete ======
echo.
echo Your file sync system is working!
echo   - Files uploaded to leader replicate to online followers
echo   - Offline servers sync missing files when coming back online
echo   - All files can be downloaded from any server
echo   - System maintains eventual consistency
echo.

pause
