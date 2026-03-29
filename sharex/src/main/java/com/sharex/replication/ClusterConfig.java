package com.sharex.replication;

import com.sharex.zookeeper.ZooKeeperService;
import java.util.*;

/**
 * Manager for cluster configuration and synchronization logic.
 * Maintains the list of all servers in the cluster and identifies the leader.
 */
public class ClusterConfig {
    private ServerConfig currentServer; // This server's configuration
    private List<ServerConfig> allServers; // All servers in the cluster
    private Map<String, ServerConfig> serverById; // Quick lookup by server ID
    private ZooKeeperService zooKeeperService; // ZooKeeper service for leader election

    public ClusterConfig(ServerConfig currentServer, List<ServerConfig> allServers) {
        this.currentServer = currentServer;
        this.allServers = allServers;
        this.serverById = new HashMap<>();
        for (ServerConfig server : allServers) {
            serverById.put(server.getServerId(), server);
        }
    }

    public ClusterConfig(ServerConfig currentServer, List<ServerConfig> allServers, ZooKeeperService zooKeeperService) {
        this(currentServer, allServers);
        this.zooKeeperService = zooKeeperService;
    }

    /**
     * Get the leader/primary server.
     * Uses ZooKeeper for dynamic leader election if available, otherwise falls back
     * to hardcoded logic.
     */
    public ServerConfig getLeader() {
        if (zooKeeperService != null && zooKeeperService.isConnected()) {
            String leaderId = zooKeeperService.getLeader();
            ServerConfig leader = serverById.get(leaderId);
            if (leader != null) {
                return leader;
            }
        }

        // Fallback: Use the static leader flag if set, otherwise default to current
        // server
        // to avoid always returning a potentially dead server1
        return allServers.stream()
                .filter(ServerConfig::isLeader)
                .findFirst()
                .orElse(currentServer);
    }

    /**
     * Get all follower/backup servers (excluding the leader).
     */
    public List<ServerConfig> getFollowers() {
        ServerConfig leader = getLeader();
        return allServers.stream()
                .filter(s -> !s.getServerId().equals(leader.getServerId()))
                .toList();
    }

    /**
     * Check if current server is the leader.
     */
    public boolean isCurrentServerLeader() {
        if (zooKeeperService != null && zooKeeperService.isConnected()) {
            return zooKeeperService.isCurrentServerLeader();
        }

        // Fallback to static configuration
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
