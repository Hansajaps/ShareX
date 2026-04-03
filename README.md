# ShareX - Distributed File Storage System

A distributed file storage system demonstrating Primary-Backup Replication with Strong Consistency in a cluster of 3 servers.

## 👥 Members
- IT24103461 – Ariyasena K P D H      [it24103461@my.sliit.lk]
- IT24103465 – Sooriyapperuma S W H P [it24103465@my.sliit.lk]
- IT24101022 – Mannapperuma M A S K M [it24101022@my.sliit.lk]
- IT24101867 – Dissanayake D M S T    [it24101867@my.sliit.lk]

## 🚀 Quick Start

### Prerequisites
- **Java 17 JDK** installed
- **Maven 3.8+** installed
- **ZooKeeper** installed and running (`zkServer.cmd` for Windows)
- **3 terminal windows** for running 3 servers

### Build
```bash
cd sharex
mvn clean package
```

### Run All Servers

**Option 1: Automated Script (Recommended)**

Windows:
```powershell
run-all-servers.bat
```

Linux/Mac:
```bash
bash run-all-servers.sh
```

**Option 2: Manual (3 terminals)**

Terminal 1 - Leader:
```bash
java -jar target/sharex-1.0.0.jar server1
```

Terminal 2 - Follower:
```bash
java -jar target/sharex-1.0.0.jar server2
```

Terminal 3 - Follower:
```bash
java -jar target/sharex-1.0.0.jar server3
```

Wait 2-3 seconds for servers to start.

--------------------------------------------------------------------------------------

### 🎯 Access Web Dashboard
Open browser to: `http://localhost:8081`

### Upload Files
1. Click **"Select File"** button
2. Choose a file from your computer
3. Click **"Upload to Leader"**
4. File automatically replicates to all servers ✅

### Download Files
1. Enter the filename (or select from the file list)
2. Choose which server to download from
3. Click **"Download"**

### View Files
- See all files on all servers in the "Files" tab
- Click **"Delete"** to remove a file from all servers
- Monitor total file count and system status

### Monitor System
- Click **"System Info"** tab to view:
  - ZooKeeper connection status (Connected/Disconnected)
  - Current leader server
  - Total files stored
  - Cluster configuration

======================================================================================

## 🔌 REST API Endpoints

### Upload File
```http
POST /upload?file=<filename>
Content-Type: multipart/form-data

Body: raw file data
```

**Example:**
```bash
curl -X POST \
  -F "data=@myfile.txt" \
  http://localhost:8081/upload?file=myfile.txt
```

**Response (Success - 200):**
```json
{
  "message": "File uploaded and replicated successfully",
  "file": "myfile.txt",
  "size": 1024,
  "replicatedTo": ["Server2", "Server3"]
}
```

**Response (Error - 403 on follower):**
```json
{
  "error": "Only the leader accepts writes"
}
```

### Download File
```http
GET /download?file=<filename>
```

**Example:**
```bash
curl http://localhost:8081/download?file=myfile.txt -o myfile.txt
curl http://localhost:8082/download?file=myfile.txt -o myfile.txt  # From follower
```

### Delete File
```http
DELETE /delete?file=<filename>
```

**Example:**
```bash
curl -X DELETE http://localhost:8081/delete?file=myfile.txt
```

**Response:**
```json
{
  "message": "File deleted from all servers",
  "file": "myfile.txt"
}
```

### Check Server Status
```http
GET /status
```

**Example:**
```bash
curl http://localhost:8081/status
```

**Response:**
```json
{
  "serverId": "Server1",
  "port": 8081,
  "isLeader": true,
  "uptime": 12345
}
```

### Get System Info
```http
GET /api/info
```

**Response:**
```json
{
  "currentServer": "Server1",
  "isLeader": true,
  "leader": "Server1",
  "zkConnected": true,
  "allServers": [
    {"id": "Server1", "port": 8081, "url": "http://localhost:8081"},
    {"id": "Server2", "port": 8082, "url": "http://localhost:8082"},
    {"id": "Server3", "port": 8083, "url": "http://localhost:8083"}
  ]
}
```

### List All Files
```http
GET /files
```

**Response:**
```json
{
  "allServers": [
    {
      "server": "Server1",
      "url": "http://localhost:8081",
      "files": [
        {"name": "file1.txt", "size": "1.0 KB"},
        {"name": "file2.pdf", "size": "2.5 MB"}
      ],
      "fileCount": 2
    },
    ...
  ],
  "totalFiles": 6
}
```

## 🧪 Testing & Validation

### Manual Testing

1. **Test replication**:
```bash
# Upload to leader
echo "Test content" > test.txt
curl -X POST -F "data=@test.txt" http://localhost:8081/upload?file=test.txt

# Download from followers (should be identical)
curl http://localhost:8082/download?file=test.txt -o test2.txt
curl http://localhost:8083/download?file=test.txt -o test3.txt

# Verify files are identical
diff test.txt test2.txt
diff test.txt test3.txt
```

2. **Test write rejection on followers**:
```bash
# Try to write to follower (should fail with 403)
curl -X POST -F "data=@test.txt" http://localhost:8082/upload?file=test.txt
# Response: {"error":"Only the leader accepts writes"}
```

3. **Test deletion**:
```bash
# Delete from leader
curl -X DELETE http://localhost:8081/delete?file=test.txt

# Verify file is gone from all servers
curl http://localhost:8081/files | grep test.txt
curl http://localhost:8082/files | grep test.txt
curl http://localhost:8083/files | grep test.txt
```

### Automated Testing

Windows:
```powershell
run-tests.bat
```

Linux/Mac:
```bash
bash run-tests.sh
```

## Consistency Model

### Write Operations (POST /upload)
- ✅ **ALLOWED**: On leader (Server 1 on port 8081)
- ❌ **REJECTED**: On followers (Server 2, Server 3) with HTTP 403 Forbidden

**Write Process**:
1. Client sends file to leader server
2. Leader saves file to local storage
3. Leader replicates file to all followers via HTTP POST
4. All writes to the same file are serialized using file-level mutexes
5. Replication is atomic (entire file or nothing)

### Read Operations (GET /download)
- ✅ **ALLOWED**: From any server (leader or follower)
- Reads are always consistent due to complete replication

## ⚙️ ZooKeeper Integration

### Overview
ShareX integrates with Apache ZooKeeper for distributed coordination and leader election. This is **optional** - the system falls back to hardcoded configuration if ZooKeeper is unavailable.

### Benefits
- **Fault Tolerance**: System continues if one ZooKeeper node fails
- **Distributed Coordination**: All servers agree on leader
- **Dynamic Configuration**: No need to restart servers for configuration changes
- **Scalability**: Quorum-based consensus

## 🔄 Automatic File Synchronization (NEW!)

### Consistency Guarantee
✅ **All online servers MUST have all files**
- When a file is uploaded to any leader, it replicates to ALL other online servers
- Offline servers receive files automatically when they come back online
- No server is left behind - full consistency guaranteed

### How It Works

**Upload to Any Leader:**
1. File uploaded to current leader (whoever is elected)
2. Leader saves file locally
3. Leader replicates to **ALL other servers** simultaneously
4. If server is offline: sync catches it when it comes online
5. **Result**: All online servers have the file immediately ✅

**Server Comes Back Online:**
1. Server registers for sync in ZooKeeper
2. Leader detects it's online
3. Leader replicates any missing files
4. Server now has all files ✅

### Example
```
Scenario: Server3 is leader, Server2 is offline, Server1 is online

1. Upload file1.txt to leader (server3)
   → Replicates to server1 ✅ (online)
   → Fails to replicate to server2 ✗ (offline)

2. Meanwhile, Server1 has file1.txt ✅

3. Server2 comes online
   → Leader detects server2 is online
   → Leader replicates file1.txt to server2 ✅
   
Result: All 3 servers have file1.txt ✅
```

### Key Guarantees

✅ **Immediate replication** - Online servers get files instantly  
✅ **No missed files** - Offline servers sync when they come back  
✅ **Works with any leader** - Replicates regardless of who is elected  
✅ **Automatic** - No manual intervention needed  
✅ **Consistent** - All servers always have all files



