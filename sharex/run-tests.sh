#!/bin/bash
# ShareX - Automated Test Script for Linux/Mac
# Tests replication, consistency, and all endpoints

echo "====================================="
echo "   ShareX - Automated Test Suite"
echo "====================================="
echo ""

# Configuration
LEADER="http://localhost:8081"
FOLLOWER1="http://localhost:8082"
FOLLOWER2="http://localhost:8083"

# Check if servers are running
echo "Checking if servers are running..."
for server in "$LEADER" "$FOLLOWER1" "$FOLLOWER2"; do
    if ! curl -s "$server/status" > /dev/null 2>&1; then
        echo "ERROR: Server $server is not responding"
        echo "Please start all servers first using: bash run-all-servers.sh"
        exit 1
    fi
done

echo "OK: All servers are running!"
echo ""
echo "====================================="
echo "Starting tests..."
echo "====================================="
echo ""

# Test 1: Check server status
echo "[TEST 1] Checking server status..."
echo ""
echo "Leader status:"
curl -s "$LEADER/status" | grep -E '"serverId"|"isLeader"'
echo ""
echo "Follower 1 status:"
curl -s "$FOLLOWER1/status" | grep -E '"serverId"|"isLeader"'
echo ""

# Test 2: Create test file
echo "[TEST 2] Creating test file..."
echo "This is a test file for ShareX replication" > testfile.txt
if [ -f testfile.txt ]; then
    echo "OK: Test file created"
else
    echo "ERROR: Failed to create test file"
    exit 1
fi
echo ""

# Test 3: Upload to leader
echo "[TEST 3] Uploading file to leader..."
response=$(curl -s -X POST -F "file=testfile.txt" -F "@testfile.txt" "$LEADER/upload")
if echo "$response" | grep -q "uploaded"; then
    echo "OK: File uploaded to leader"
else
    echo "ERROR: Upload failed"
    echo "Response: $response"
    exit 1
fi
echo ""

# Test 4: Verify replication on followers
echo "[TEST 4] Verifying replication to followers..."
curl -s "$LEADER/download?file=testfile.txt" > file_leader.txt
curl -s "$FOLLOWER1/download?file=testfile.txt" > file_follower1.txt
curl -s "$FOLLOWER2/download?file=testfile.txt" > file_follower2.txt

if diff file_leader.txt file_follower1.txt > /dev/null 2>&1; then
    echo "OK: Files are identical on leader and follower1"
else
    echo "ERROR: Files on leader and follower1 differ"
    exit 1
fi

if diff file_leader.txt file_follower2.txt > /dev/null 2>&1; then
    echo "OK: Files are identical on leader and follower2"
else
    echo "ERROR: Files on leader and follower2 differ"
    exit 1
fi
echo ""

# Test 5: Test write rejection on followers
echo "[TEST 5] Testing write rejection on followers..."
response=$(curl -s -X POST -F "file=fail_test.txt" -F "data=should fail" "$FOLLOWER1/upload")
if echo "$response" | grep -q -i "forbidden\|error"; then
    echo "OK: Follower correctly rejected write attempt"
else
    echo "ERROR: Follower should have rejected write but didn't"
    exit 1
fi
echo ""

# Test 6: Multiple file replication
echo "[TEST 6] Testing multiple file replication..."
for i in 1 2 3; do
    echo "Testing multiple files - file $i"
    echo "Content of file $i" > "file_$i.txt"
    curl -s -X POST -F "file=file_$i.txt" -F "@file_$i.txt" "$LEADER/upload" > /dev/null 2>&1
done

# Verify all files replicated
all_ok=true
for i in 1 2 3; do
    if ! curl -s "$FOLLOWER1/download?file=file_$i.txt" > /dev/null 2>&1; then
        all_ok=false
    fi
done

if [ "$all_ok" = "true" ]; then
    echo "OK: All multiple files replicated successfully"
else
    echo "ERROR: Some files failed to replicate"
fi
echo ""

# Test 7: Large file handling
echo "[TEST 7] Testing large file upload..."
# Create a 5MB test file
dd if=/dev/urandom of=large_file.bin bs=1M count=5 2>/dev/null
response=$(curl -s -X POST -F "file=large_file.bin" -F "@large_file.bin" "$LEADER/upload")
if echo "$response" | grep -q "uploaded"; then
    echo "OK: Large file uploaded successfully"
else
    echo "ERROR: Large file upload failed"
fi
echo ""

# Cleanup
echo "====================================="
echo "Cleaning up test files..."
echo "====================================="
rm -f testfile.txt file_leader.txt file_follower1.txt file_follower2.txt large_file.bin
for i in 1 2 3; do
    rm -f "file_$i.txt"
done

echo ""
echo "====================================="
echo "All tests completed!"
echo "====================================="
echo ""
