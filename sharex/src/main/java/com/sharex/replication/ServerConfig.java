package com.sharex.replication;

/**
 * Configuration for a single server node in the ShareX cluster.
 * Contains server identification, port, and storage location information.
 */
public class ServerConfig {
    private String serverId;      // Unique identifier (e.g., "server1")
    private int port;             // HTTP port this server runs on
    private String host;          // Hostname or IP
    private String storagePath;   // Local filesystem path for storing files
    private boolean isLeader;     // True if this is the primary/leader server

    public ServerConfig(String serverId, int port, String host, String storagePath, boolean isLeader) {
        this.serverId = serverId;
        this.port = port;
        this.host = host;
        this.storagePath = storagePath;
        this.isLeader = isLeader;
    }

    // Getters and Setters
    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public String getBaseUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return "ServerConfig{" +
                "serverId='" + serverId + '\'' +
                ", port=" + port +
                ", host='" + host + '\'' +
                ", storagePath='" + storagePath + '\'' +
                ", isLeader=" + isLeader +
                '}';
    }
}
