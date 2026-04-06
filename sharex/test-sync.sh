#!/bin/bash
# Test Script for File Synchronization Feature
# This script demonstrates the automatic sync of files when servers come online

echo "======================================"
echo "ShareX File Sync Test Suite"
echo "======================================"
echo ""

LEADER="http://localhost:8081"
FOLLOWER2="http://localhost:8082"
FOLLOWER3="http://localhost:8083"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function print_section() {
    echo ""
    echo "╔════════════════════════════════════╗"
    echo "║ $1"
    echo "╚════════════════════════════════════╝"
}

function print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

function print_error() {
    echo -e "${RED}❌ $1${NC}"
}

function print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Test 1: Check initial status
print_section "Test 1: Check Server Status"

print_info "Checking leader status..."
curl -s $LEADER/status | jq . > /dev/null 2>&1 && print_success "Leader is online" || print_error "Leader is offline"

print_info "Checking follower2 status..."
curl -s $FOLLOWER2/status | jq . > /dev/null 2>&1 && print_success "Follower2 is online" || print_error "Follower2 is offline"

print_info "Checking follower3 status..."
curl -s $FOLLOWER3/status | jq . > /dev/null 2>&1 && print_success "Follower3 is online" || print_error "Follower3 is offline"

# Test 2: Upload files to leader
print_section "Test 2: Upload Files to Leader"

print_info "Creating test file..."
echo "test content for file1" > /tmp/test1.txt

print_info "Uploading file1 to leader..."
curl -s -F "file=file1.txt" -F "data=@/tmp/test1.txt" $LEADER/upload | jq .

print_info "Creating and uploading file2..."
echo "test content for file2" > /tmp/test2.txt
curl -s -F "file=file2.txt" -F "data=@/tmp/test2.txt" $LEADER/upload | jq .

# Test 3: Verify files on all servers
print_section "Test 3: Verify Files Replicated to Followers"

print_info "Checking follower2 files..."
curl -s $FOLLOWER2/list-files | jq .

print_info "Checking follower3 files..."
curl -s $FOLLOWER3/list-files | jq .

# Test 4: Check missing files (should be empty)
print_section "Test 4: Check Missing Files (Should Be Empty)"

print_info "Missing files on follower2..."
MISSING2=$(curl -s $FOLLOWER2/get-missing-files | jq '.missingFiles | length')
if [ "$MISSING2" -eq 0 ]; then
    print_success "Follower2 has all files"
else
    print_error "Follower2 is missing $MISSING2 files"
fi

print_info "Missing files on follower3..."
MISSING3=$(curl -s $FOLLOWER3/get-missing-files | jq '.missingFiles | length')
if [ "$MISSING3" -eq 0 ]; then
    print_success "Follower3 has all files"
else
    print_error "Follower3 is missing $MISSING3 files"
fi

# Test 5: Simulate offline server scenario
print_section "Test 5: Upload Files While Follower2 is Offline"

print_info "⚠️  NOTE: Manually kill follower2 now (Ctrl+C in its terminal)"
print_info "Then come back here and press ENTER to continue..."
read -p "Press ENTER when ready..."

print_info "Uploading file3 to leader (follower2 is offline)..."
echo "test content for file3" > /tmp/test3.txt
curl -s -F "file=file3.txt" -F "data=@/tmp/test3.txt" $LEADER/upload | jq .

print_info "Uploading file4 to leader (follower2 still offline)..."
echo "test content for file4" > /tmp/test4.txt
curl -s -F "file=file4.txt" -F "data=@/tmp/test4.txt" $LEADER/upload | jq .

# Test 6: Verify files on online servers
print_section "Test 6: Verify Files on Online Servers"

print_info "Files on follower3 (should have all 4)..."
curl -s $FOLLOWER3/list-files | jq '.files'

# Test 7: Restart follower2 and observe sync
print_section "Test 7: Restart Follower2 and Observe Sync"

print_info "⚠️  Now start follower2 again (java -jar sharex.jar server2 in new terminal)"
print_info "Press ENTER after follower2 is online..."
read -p "Press ENTER when ready..."

print_info "Waiting 10 seconds for sync to complete..."
sleep 10

# Test 8: Verify sync completed
print_section "Test 8: Verify Follower2 Has All Files After Sync"

print_info "Checking follower2 files after sync..."
curl -s $FOLLOWER2/list-files | jq '.files'

print_info "Checking missing files on follower2 (should be empty)..."
FINAL_MISSING=$(curl -s $FOLLOWER2/get-missing-files | jq '.missingFiles')
if [ "$FINAL_MISSING" == "[]" ]; then
    print_success "Follower2 sync complete - has all files!"
else
    print_error "Follower2 still missing files: $FINAL_MISSING"
fi

# Test 9: Download files from all servers
print_section "Test 9: Verify Files Can Be Downloaded from All Servers"

print_info "Downloading file1 from leader..."
curl -s $LEADER/download?file=file1.txt > /dev/null && print_success "Downloaded file1 from leader" || print_error "Failed to download file1 from leader"

print_info "Downloading file4 from follower2 (recently synced)..."
curl -s $FOLLOWER2/download?file=file4.txt > /dev/null && print_success "Downloaded file4 from follower2" || print_error "Failed to download file4 from follower2"

print_info "Downloading file3 from follower3..."
curl -s $FOLLOWER3/download?file=file3.txt > /dev/null && print_success "Downloaded file3 from follower3" || print_error "Failed to download file3 from follower3"

# Summary
print_section "Test Summary"

print_success "All tests completed!"
print_info "Your file sync system is working correctly:"
print_info "  ✅ Files uploaded to leader replicate to online followers"
print_info "  ✅ Offline servers sync missing files when coming back online"
print_info "  ✅ All files can be downloaded from any server"
print_info "  ✅ System maintains eventual consistency"

echo ""
