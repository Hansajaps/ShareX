package com.sharex.replication;

import java.util.*;

/**
 * Manager for cluster configuration and synchronization logic.
 * Maintains the list of all servers in the cluster and identifies the leader.
 */
public class ClusterConfig {
    private ServerConfig currentServer;      // This server's configuration
    private List<ServerConfig> allServers;   // All servers in the cluster
    private Map<String, ServerConfig> serverById; // Quick lookup by server ID

    public ClusterConfig(ServerConfig currentServer, List<ServerConfig> allServers) {
        this.currentServer = currentServer;
        this.allServers = allServers;
        this.serverById = new HashMap<>();
        for (ServerConfig server : allServers) {
            serverById.put(server.getServerId(), server);
        }
    }

    /**
     * Get the leader/primary server.
     * Currently assumes the first server in the cluster is the leader.
     * In a real system, this would be determined by a consensus algorithm.
     */
    public ServerConfig getLeader() {
        return allServers.stream()
                .filter(ServerConfig::isLeader)
                .findFirst()
                .orElse(allServers.get(0)); // Fallback: first server is leader
    }

    /**
     * Get all follower/backup servers (excluding the leader).
     */
    public List<ServerConfig> getFollowers() {
        return allServers.stream()
                .filter(s -> !s.isLeader())
                .toList();
    }

    /**
     * Check if current server is the leader.
     */
    public boolean isCurrentServerLeader() {
        return currentServer.isLeader();
    }

    /**
     * Get current server configuration.
     */
    public ServerConfig getCurrentServer() {
        return currentServer;
    }

    /**
     * Get all servers.
     */
    public List<ServerConfig> getAllServers() {
        return new ArrayList<>(allServers);
    }

    /**
     * Get server by ID.
     */
    public ServerConfig getServerById(String serverId) {
        return serverById.get(serverId);
    }
}
