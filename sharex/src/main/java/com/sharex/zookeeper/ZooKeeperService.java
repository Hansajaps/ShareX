package com.sharex.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ZooKeeper Service for ShareX
 * Handles leader election, service discovery, distributed coordination, and
 * file metadata.
 * 
 * The service supports both standalone and clustered ZooKeeper modes:
 * - Standalone: Single ZooKeeper instance at localhost:2181
 * - Cluster: 3-node quorum at localhost:2181, localhost:2182, localhost:2183
 */
public class ZooKeeperService {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperService.class);
    private static final String SERVERS_PATH = "/sharex/servers";
    private static final String LEADER_PATH = "/sharex/leader";
    private static final String ELECTION_PATH = "/sharex/election";
    private static final String FILES_PATH = "/sharex/files";
    private static final String SERVER_SYNC_PATH = "/sharex/server-sync";
    public static final String RAFT_STATE_PATH = "/sharex/raft/state";

    // ZooKeeper connection string for 3-node cluster
    // If only one ZooKeeper is running, it will default to localhost:2181
    private static final String ZK_CLUSTER_HOSTS = "localhost:2181,localhost:2182,localhost:2183";

    private CuratorFramework client;
    private String serverId;
    private int serverPort;
    private boolean zkConnected = false;
    private LeaderSelector leaderSelector;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private LeaderElectionCallback leaderCallback;
    private ServerSyncCallback syncCallback;

    /**
     * Callback interface for server sync/online events
     */
    public interface ServerSyncCallback {
        void onServerOnline(String serverId);
    }

    /**
     * Callback interface for leader election events
     */
    public interface LeaderElectionCallback {
        void onLeadershipGained();

        void onLeadershipLost();
    }

    public ZooKeeperService(String serverId, int serverPort) {
        this.serverId = serverId;
        this.serverPort = serverPort;
    }

    public ZooKeeperService(String serverId, int serverPort, LeaderElectionCallback callback) {
        this.serverId = serverId;
        this.serverPort = serverPort;
        this.leaderCallback = callback;
    }

    public ZooKeeperService(String serverId, int serverPort, LeaderElectionCallback leaderCallback,
            ServerSyncCallback syncCallback) {
        this.serverId = serverId;
        this.serverPort = serverPort;
        this.leaderCallback = leaderCallback;
        this.syncCallback = syncCallback;
    }

    /**
     * Initialize ZooKeeper connection and register this server.
     * Tries to connect to the 3-node cluster first, then falls back to single node.
     */
    public void start() {
        try {
            logger.info("Connecting to ZooKeeper cluster: {}", ZK_CLUSTER_HOSTS);

            client = CuratorFrameworkFactory.builder()
                    .connectString(ZK_CLUSTER_HOSTS)
                    .retryPolicy(new ExponentialBackoffRetry(500, 3))
                    .connectionTimeoutMs(3000)   // Detect dead nodes faster
                    .sessionTimeoutMs(4000)      // Trigger re-election within 4s
                    .build();

            // Add listener to initialize once connected
            client.getConnectionStateListenable().addListener((c, newState) -> {
                if (newState.isConnected()) {
                    logger.info("✅ Connected to ZooKeeper cluster");
                    zkConnected = true;
                    onConnected();
                } else {
                    logger.warn("❌ Disconnected from ZooKeeper");
                    zkConnected = false;
                }
            });

            client.start();
            logger.info("ZooKeeper client started, waiting for connection...");
        } catch (Exception e) {
            logger.error("❌ Failed to start ZooKeeper client", e);
            zkConnected = false;
        }
    }

    /**
     * Called when connection is established
     */
    private void onConnected() {
        try {
            // Create root paths if not exist
            createPathIfNotExists(SERVERS_PATH);
            createPathIfNotExists(LEADER_PATH);
            createPathIfNotExists(ELECTION_PATH);
            createPathIfNotExists(FILES_PATH);
            createPathIfNotExists(SERVER_SYNC_PATH);
            createPathIfNotExists(RAFT_STATE_PATH);

            // Register this server
            registerServer();

            // Start leader election
            startLeaderElection();

            logger.info("✅ Server {} initialized in ZooKeeper", serverId);
        } catch (Exception e) {
            logger.error("Error during ZooKeeper initialization", e);
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
     * Start leader election using Curator's LeaderSelector
     */
    private void startLeaderElection() {
        try {
            leaderSelector = new LeaderSelector(client, ELECTION_PATH, new LeaderSelectorListenerAdapter() {
                @Override
                public void takeLeadership(CuratorFramework client) throws Exception {
                    // This method is called when we become the leader
                    logger.info("🎯 Server {} has become the LEADER!", serverId);
                    isLeader.set(true);

                    // Update the leader znode
                    setLeader(serverId);

                    // Notify callback if provided
                    if (leaderCallback != null) {
                        leaderCallback.onLeadershipGained();
                    }

                    // Keep leadership until we lose it or the process ends
                    try {
                        Thread.currentThread().join(); // Block until leadership is lost
                    } catch (InterruptedException e) {
                        logger.info("Leadership interrupted for server {}", serverId);
                        Thread.currentThread().interrupt();
                    }
                }
            });

            // Set the participant ID to server ID for deterministic ordering
            leaderSelector.setId(serverId);

            // Start the leader election
            leaderSelector.start();

            logger.info("Started leader election for server {}", serverId);
        } catch (Exception e) {
            logger.error("Failed to start leader election", e);
        }
    }

    /**
     * Store file metadata in ZooKeeper when a file is uploaded.
     * This allows other servers to know about uploaded files.
     */
    public void registerFileMetadata(String filename, long fileSize, long timestamp) {
        if (!zkConnected)
            return;

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
        if (!zkConnected)
            return new ArrayList<>();

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
        if (!zkConnected)
            return;

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
     * Save Raft Consensus State (Term and VotedFor) to ZooKeeper.
     * Format: currentTerm|votedFor
     */
    public void saveRaftState(String serverId, int term, String votedFor) {
        if (!zkConnected) return;
        try {
            String path = RAFT_STATE_PATH + "/" + serverId;
            String data = term + "|" + (votedFor != null ? votedFor : "null");
            
            if (client.checkExists().forPath(path) != null) {
                client.setData().forPath(path, data.getBytes(StandardCharsets.UTF_8));
            } else {
                client.create().creatingParentsIfNeeded().forPath(path, data.getBytes(StandardCharsets.UTF_8));
            }
            logger.debug("Saved Raft state for {} in ZooKeeper: {}", serverId, data);
        } catch (Exception e) {
            logger.error("Failed to save Raft state to ZooKeeper for node {}", serverId, e);
        }
    }

    /**
     * Get Raft Consensus State from ZooKeeper.
     */
    public String getRaftState(String serverId) {
        if (!zkConnected) return null;
        try {
            String path = RAFT_STATE_PATH + "/" + serverId;
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                return new String(data, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.debug("No Raft state found in ZooKeeper for node {}", serverId);
        }
        return null;
    }

    /**
     * Get data for a specific server from /sharex/servers/
     */
    public String getServerUrl(String serverId) {
        if (!zkConnected) return null;
        try {
            String path = SERVERS_PATH + "/" + serverId;
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                return new String(data, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.debug("Could not find server URL in ZooKeeper for node {}", serverId);
        }
        return null;
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
                return null; // No ZK, no dynamic leader
            }

            if (client.checkExists().forPath(LEADER_PATH) != null) {
                byte[] data = client.getData().forPath(LEADER_PATH);
                if (data == null || data.length == 0)
                    return null;

                String leader = new String(data, StandardCharsets.UTF_8);
                logger.debug("Current leader: {}", leader);
                return leader;
            }
        } catch (Exception e) {
            logger.debug("Could not fetch leader from ZooKeeper", e);
        }
        return null;
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
                client.create().withMode(CreateMode.EPHEMERAL).forPath(LEADER_PATH,
                        leaderData.getBytes(StandardCharsets.UTF_8));
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
     * Check if this server is currently the leader
     */
    public boolean isCurrentServerLeader() {
        return isLeader.get();
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
     * Register this server as requiring sync (call after connecting/coming online)
     * Used to notify the leader to synchronize missing files
     */
    public void registerServerForSync(long timestamp) {
        if (!zkConnected)
            return;

        try {
            String syncPath = SERVER_SYNC_PATH + "/" + serverId;
            String syncData = String.valueOf(timestamp);

            if (client.checkExists().forPath(syncPath) != null) {
                client.setData().forPath(syncPath, syncData.getBytes(StandardCharsets.UTF_8));
            } else {
                client.create().forPath(syncPath, syncData.getBytes(StandardCharsets.UTF_8));
            }

            logger.info("Server {} registered for sync with logical timestamp {}", serverId, timestamp);
        } catch (Exception e) {
            logger.debug("Could not register server for sync in ZooKeeper", e);
        }
    }

    /**
     * Get servers registered for sync (servers that need file synchronization)
     */
    public List<String> getServersRequiringSync() {
        if (!zkConnected)
            return new ArrayList<>();

        try {
            if (client.checkExists().forPath(SERVER_SYNC_PATH) != null) {
                return client.getChildren().forPath(SERVER_SYNC_PATH);
            }
        } catch (Exception e) {
            logger.debug("Could not get servers requiring sync from ZooKeeper", e);
        }
        return new ArrayList<>();
    }

    /**
     * Remove server from sync registry (call after sync completion)
     */
    public void unregisterServerFromSync(String serverId) {
        if (!zkConnected)
            return;

        try {
            String syncPath = SERVER_SYNC_PATH + "/" + serverId;
            if (client.checkExists().forPath(syncPath) != null) {
                client.delete().forPath(syncPath);
                logger.info("Server {} removed from sync registry", serverId);
            }
        } catch (Exception e) {
            logger.debug("Could not unregister server from sync in ZooKeeper", e);
        }
    }

    /**
     * Set the sync callback
     */
    public void setSyncCallback(ServerSyncCallback callback) {
        this.syncCallback = callback;
    }

    public void stop() {
        try {
            if (leaderSelector != null) {
                leaderSelector.close();
                logger.info("Leader selector closed");
            }

            if (client != null) {
                client.close();
                logger.info("ZooKeeper connection closed");
            }
        } catch (Exception e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }

    private boolean simulatedCrash = false;

    public void simulateCrash() {
        if (!simulatedCrash) {
            logger.warn("💥 SIMULATION: Crashing Server {} - disconnecting ZooKeeper", serverId);
            simulatedCrash = true;
            // If we are the leader, explicitly clear the leader node so followers
            // know to start a new election immediately (instead of waiting for session expiry)
            if (isLeader.get()) {
                try {
                    if (client != null && client.getZookeeperClient().isConnected()) {
                        if (client.checkExists().forPath(LEADER_PATH) != null) {
                            client.delete().forPath(LEADER_PATH);
                            logger.info("Cleared ZooKeeper leader node to trigger re-election");
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Could not clear leader node during crash simulation", e);
                }
                if (leaderCallback != null) {
                    leaderCallback.onLeadershipLost();
                }
            }
            isLeader.set(false);
            zkConnected = false;
            stop(); // Close session — triggers ZooKeeper to release all ephemeral nodes
        }
    }

    public void recoverFromCrash() {
        if (simulatedCrash) {
            logger.info("🔧 SIMULATION: Recovering Server {} - reconnecting ZooKeeper", serverId);
            simulatedCrash = false;
            start(); // Re-initialize client
        }
    }

    public boolean isSimulatedCrash() {
        return simulatedCrash;
    }
}
