#!/bin/bash
# ShareX - Start All Servers on Linux/Mac
# This script starts all 3 servers in the background

echo "====================================="
echo "   ShareX - Distributed File Storage"
echo "         Linux/Mac Launcher"
echo "====================================="
echo ""

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven is not installed or not in PATH"
    echo "Please install Maven 3.8+ first"
    exit 1
fi

# Create storage directories
echo "Creating storage directories..."
mkdir -p storage/server1
mkdir -p storage/server2
mkdir -p storage/server3

# Check if JAR file exists
if [ ! -f "target/sharex-1.0.0.jar" ]; then
    echo "JAR file not found. Building project..."
    mvn clean package -q
    if [ $? -ne 0 ]; then
        echo "ERROR: Maven build failed"
        exit 1
    fi
fi

echo ""
echo "Starting servers..."
echo ""

# Start Server 1 (Leader) - Port 8081
echo "Starting Server 1 (Leader) on port 8081..."
java -jar "target/sharex-1.0.0.jar" server1 > logs/server1.log 2>&1 &
SERVER1_PID=$!

# Wait for server1 to start
sleep 3

# Start Server 2 (Follower) - Port 8082
echo "Starting Server 2 (Follower) on port 8082..."
java -jar "target/sharex-1.0.0.jar" server2 > logs/server2.log 2>&1 &
SERVER2_PID=$!

# Wait for server2 to start
sleep 2

# Start Server 3 (Follower) - Port 8083
echo "Starting Server 3 (Follower) on port 8083..."
java -jar "target/sharex-1.0.0.jar" server3 > logs/server3.log 2>&1 &
SERVER3_PID=$!

echo ""
echo "====================================="
echo "All servers started!"
echo ""
echo "Leader (Server 1):   http://localhost:8081"
echo "Follower (Server 2): http://localhost:8082"
echo "Follower (Server 3): http://localhost:8083"
echo ""
echo "Server PIDs:"
echo "  Server 1: $SERVER1_PID"
echo "  Server 2: $SERVER2_PID"
echo "  Server 3: $SERVER3_PID"
echo ""
echo "Check server status:"
echo "  curl http://localhost:8081/status"
echo ""
echo "View logs:"
echo "  tail -f logs/server1.log"
echo "  tail -f logs/server2.log"
echo "  tail -f logs/server3.log"
echo ""
echo "To stop the servers, run:"
echo "  kill $SERVER1_PID $SERVER2_PID $SERVER3_PID"
echo "====================================="
