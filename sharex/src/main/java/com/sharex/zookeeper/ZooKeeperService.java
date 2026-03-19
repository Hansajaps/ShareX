package com.sharex.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * ZooKeeper Service for ShareX
 * Handles leader election, service discovery, distributed coordination, and file metadata.
 * 
 * The service supports both standalone and clustered ZooKeeper modes:
 * - Standalone: Single ZooKeeper instance at localhost:2181
 * - Cluster: 3-node quorum at localhost:2181, localhost:2182, localhost:2183
 */
public class ZooKeeperService {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperService.class);
    private static final String SERVERS_PATH = "/sharex/servers";
    private static final String LEADER_PATH = "/sharex/leader";
    private static final String FILES_PATH = "/sharex/files";
    
    // ZooKeeper connection string for 3-node cluster
    // If only one ZooKeeper is running, it will default to localhost:2181
    private static final String ZK_CLUSTER_HOSTS = "localhost:2181,localhost:2182,localhost:2183";
    private static final String ZK_FALLBACK_HOST = "localhost:2181";
    
    private CuratorFramework client;
    private String serverId;
    private int serverPort;
    private boolean zkConnected = false;
    
    public ZooKeeperService(String serverId, int serverPort) {
        this.serverId = serverId;
        this.serverPort = serverPort;
    }
    
    /**
     * Initialize ZooKeeper connection and register this server.
     * Tries to connect to the 3-node cluster first, then falls back to single node.
     */
    public void start() {
        try {
            logger.info("Attempting to connect to ZooKeeper cluster: {}", ZK_CLUSTER_HOSTS);
            
            client = CuratorFrameworkFactory.newClient(
                    ZK_CLUSTER_HOSTS,
                    new ExponentialBackoffRetry(1000, 3)
            );
            client.start();
            client.blockUntilConnected();
            
            logger.info("✅ Connected to ZooKeeper cluster");
            zkConnected = true;
            
            // Create root paths if not exist
            createPathIfNotExists(SERVERS_PATH);
            createPathIfNotExists(LEADER_PATH);
            createPathIfNotExists(FILES_PATH);
            
            // Register this server
            registerServer();
            
            logger.info("✅ Server {} registered in ZooKeeper", serverId);
        } catch (Exception e) {
            logger.warn("❌ Could not connect to ZooKeeper cluster, trying fallback: {}", e.getMessage());
            tryFallbackConnection();
        }
    }
    
    /**
     * Fallback connection to single ZooKeeper instance
     */
    private void tryFallbackConnection() {
        try {
            logger.info("Attempting fallback connection to single ZooKeeper at {}", ZK_FALLBACK_HOST);
            
            client = CuratorFrameworkFactory.newClient(
                    ZK_FALLBACK_HOST,
                    new ExponentialBackoffRetry(1000, 3)
            );
            client.start();
            client.blockUntilConnected();
            
            logger.info("✅ Connected to ZooKeeper (fallback single mode)");
            zkConnected = true;
            
            // Create paths
            createPathIfNotExists(SERVERS_PATH);
            createPathIfNotExists(LEADER_PATH);
            createPathIfNotExists(FILES_PATH);
            
            registerServer();
            logger.info("✅ Server {} registered in ZooKeeper (fallback mode)", serverId);
        } catch (Exception e) {
            logger.warn("⚠️  ZooKeeper not available. Running without distributed coordination.");
            logger.warn("   System will use hardcoded server configuration as fallback.");
            zkConnected = false;
        }
    }
    
    /**
     * Register this server in ZooKeeper
     */
    private void registerServer() {
        try {
            String serverPath = SERVERS_PATH + "/" + serverId;
            String serverData = "localhost:" + serverPort;
            
            if (client.checkExists().forPath(serverPath) != null) {
                client.setData().forPath(serverPath, serverData.getBytes(StandardCharsets.UTF_8));
                logger.debug("Updated server {} in ZooKeeper", serverId);
            } else {
                client.create().forPath(serverPath, serverData.getBytes(StandardCharsets.UTF_8));
                logger.debug("Registered server {} in ZooKeeper", serverId);
            }
        } catch (Exception e) {
            logger.error("Failed to register server in ZooKeeper", e);
        }
    }
    
    /**
     * Store file metadata in ZooKeeper when a file is uploaded.
     * This allows other servers to know about uploaded files.
     */
    public void registerFileMetadata(String filename, long fileSize, long timestamp) {
        if (!zkConnected) return;
        
        try {
            String filePath = FILES_PATH + "/" + filename;
            String metadata = String.format("%d|%d|%s", fileSize, timestamp, serverId);
            
            if (client.checkExists().forPath(filePath) != null) {
                client.setData().forPath(filePath, metadata.getBytes(StandardCharsets.UTF_8));
                logger.debug("Updated file metadata in ZooKeeper: {}", filename);
            } else {
                client.create().forPath(filePath, metadata.getBytes(StandardCharsets.UTF_8));
                logger.debug("Registered file metadata in ZooKeeper: {}", filename);
            }
        } catch (Exception e) {
            logger.debug("Could not register file metadata in ZooKeeper", e);
        }
    }
    
    /**
     * Get list of all files from ZooKeeper metadata
     */
    public List<String> getRegisteredFiles() {
        if (!zkConnected) return new ArrayList<>();
        
        try {
            if (client.checkExists().forPath(FILES_PATH) != null) {
                return client.getChildren().forPath(FILES_PATH);
            }
        } catch (Exception e) {
            logger.debug("Could not get file list from ZooKeeper", e);
        }
        return new ArrayList<>();
    }
    
    /**
     * Remove file metadata from ZooKeeper when file is deleted
     */
    public void unregisterFileMetadata(String filename) {
        if (!zkConnected) return;
        
        try {
            String filePath = FILES_PATH + "/" + filename;
            if (client.checkExists().forPath(filePath) != null) {
                client.delete().forPath(filePath);
                logger.debug("Removed file metadata from ZooKeeper: {}", filename);
            }
        } catch (Exception e) {
            logger.debug("Could not remove file metadata from ZooKeeper", e);
        }
    }

    /**
     * Get all registered servers in the cluster
     */
    public List<String> getAllServers() {
        try {
            if (client == null || !client.getZookeeperClient().isConnected()) {
                return getDefaultServers();
            }
            
            List<String> servers = client.getChildren().forPath(SERVERS_PATH);
            logger.debug("Found {} servers in ZooKeeper", servers.size());
            return servers;
        } catch (Exception e) {
            logger.debug("Could not fetch servers from ZooKeeper, using defaults", e);
            return getDefaultServers();
        }
    }
    
    /**
     * Get all server details
     */
    public Map<String, String> getAllServerDetails() {
        Map<String, String> details = new HashMap<>();
        try {
            if (client == null || !client.getZookeeperClient().isConnected()) {
                return getDefaultServerDetails();
            }
            
            List<String> servers = getAllServers();
            for (String server : servers) {
                try {
                    byte[] data = client.getData().forPath(SERVERS_PATH + "/" + server);
                    String serverData = new String(data, StandardCharsets.UTF_8);
                    details.put(server, serverData);
                } catch (Exception e) {
                    logger.debug("Could not fetch data for server {}", server, e);
                }
            }
        } catch (Exception e) {
            logger.debug("Error fetching server details from ZooKeeper", e);
            return getDefaultServerDetails();
        }
        return details;
    }
    
    /**
     * Get the current leader
     */
    public String getLeader() {
        try {
            if (client == null || !client.getZookeeperClient().isConnected()) {
                return "server1"; // Default fallback
            }
            
            if (client.checkExists().forPath(LEADER_PATH) != null) {
                byte[] data = client.getData().forPath(LEADER_PATH);
                String leader = new String(data, StandardCharsets.UTF_8);
                logger.debug("Current leader: {}", leader);
                return leader;
            }
        } catch (Exception e) {
            logger.debug("Could not fetch leader from ZooKeeper", e);
        }
        return "server1"; // Default to server1 if ZK not available
    }
    
    /**
     * Set the leader (should only be called by leader election logic)
     */
    public void setLeader(String leaderId) {
        try {
            if (client == null || !client.getZookeeperClient().isConnected()) {
                return;
            }
            
            String leaderData = leaderId;
            if (client.checkExists().forPath(LEADER_PATH) != null) {
                client.setData().forPath(LEADER_PATH, leaderData.getBytes(StandardCharsets.UTF_8));
            } else {
                client.create().forPath(LEADER_PATH, leaderData.getBytes(StandardCharsets.UTF_8));
            }
            logger.info("Leader set to: {}", leaderId);
        } catch (Exception e) {
            logger.warn("Could not set leader in ZooKeeper", e);
        }
    }
    
    /**
     * Check if ZooKeeper is connected
     */
    public boolean isConnected() {
        try {
            return client != null && client.getZookeeperClient().isConnected();
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Create path if it doesn't exist
     */
    private void createPathIfNotExists(String path) {
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
                logger.debug("Created ZooKeeper path: {}", path);
            }
        } catch (Exception e) {
            logger.debug("Could not create ZooKeeper path {}: {}", path, e.getMessage());
        }
    }
    
    /**
     * Default servers when ZooKeeper is not available
     */
    private List<String> getDefaultServers() {
        return Arrays.asList("server1", "server2", "server3");
    }
    
    /**
     * Default server details when ZooKeeper is not available
     */
    private Map<String, String> getDefaultServerDetails() {
        Map<String, String> details = new HashMap<>();
        details.put("server1", "localhost:8081");
        details.put("server2", "localhost:8082");
        details.put("server3", "localhost:8083");
        return details;
    }
    
    /**
     * Close ZooKeeper connection
     */
    public void stop() {
        try {
            if (client != null) {
                client.close();
                logger.info("ZooKeeper connection closed");
            }
        } catch (Exception e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }
}
