# ShareX - Distributed File Storage System

A distributed file storage system demonstrating Primary-Backup Replication with Strong Consistency in a cluster of 3 servers.

## 👥 Members
- IT24103461 – Ariyasena K P D H      [it24103461@my.sliit.lk]
- IT24103465 – Sooriyapperuma S W H P [it24103465@my.sliit.lk]
- IT24101022 – Mannapperuma M A S K M [it24101022@my.sliit.lk]
- IT24101867 – Dissanayake D S M T    [it24101867@my.sliit.lk]

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

