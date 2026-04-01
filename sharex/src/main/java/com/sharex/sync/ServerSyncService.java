package com.sharex.sync;

import com.sharex.replication.ClusterConfig;
import com.sharex.replication.ServerConfig;
import com.sharex.replication.ReplicaClient;
import com.sharex.server.FileService;
import com.sharex.zookeeper.ZooKeeperService;
import com.sharex.time.TimeSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Service for synchronizing files when servers come online.
 * 
 * This service:
 * 1. Registers the current server for sync when it starts
 * 2. Monitors servers requiring sync (ZooKeeper)
 * 3. When a server is detected needing sync, the leader replicates missing
 * files
 * 4. Ensures eventual consistency across all servers
 */
@Service
public class ServerSyncService {
    private static final Logger logger = LoggerFactory.getLogger(ServerSyncService.class);
    private static final int SYNC_CHECK_INTERVAL_MS = 5000; // Check every 5 seconds
    private static final int TIMEOUT_MS = 10000; // 10 second timeout for HTTP calls

    @Autowired
    private ZooKeeperService zooKeeperService;

    @Autowired
    private ClusterConfig clusterConfig;

    @Autowired
    private FileService fileService;

    @Autowired
    private TimeSyncService timeSyncService;

    private ScheduledExecutorService syncExecutor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Auto-initialize after Spring dependency injection
     */
    @PostConstruct
    public void postConstruct() {
        initialize();
    }

    /**
     * Initialize sync service and start monitoring
     */
    public void initialize() {
        try {
            // Register this server as requiring sync
            registerServerForSync();

            // Start background sync monitor (only on leader)
            startSyncMonitor();

            logger.info("✅ Server sync service initialized");
        } catch (Exception e) {
            logger.error("Error initializing server sync service", e);
        }
    }

    /**
     * Register current server for sync (called on startup)
     */
    private void registerServerForSync() {
        try {
            // Register using the synchronized logical time
            zooKeeperService.registerServerForSync(timeSyncService.getCurrentTime());
            logger.info("✅ Server {} registered for sync", clusterConfig.getCurrentServer().getServerId());
        } catch (Exception e) {
            logger.warn("Could not register for sync: {}", e.getMessage());
        }
    }

    /**
     * Start background task to monitor and sync servers
     * Only the leader performs active sync replication
     */
    private void startSyncMonitor() {
        if (syncExecutor != null) {
            return; // Already started
        }

        syncExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "ServerSyncMonitor");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic sync checks
        syncExecutor.scheduleAtFixedRate(
                this::checkAndSyncServers,
                0, // Start immediately
                SYNC_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        logger.info("Started server sync monitor");
    }

    /**
     * Check for any servers in the cluster that might be missing files and sync
     * them.
     * The leader proactively ensures all other nodes are consistent with the
     * cluster metadata.
     */
    private void checkAndSyncServers() {
        try {
            // Only leader performs master sync coordination
            if (!clusterConfig.isCurrentServerLeader()) {
                return;
            }

            // Get all servers in the configuration
            List<ServerConfig> allServers = clusterConfig.getAllServers();
            String currentServerId = clusterConfig.getCurrentServer().getServerId();

            // Proactively check each server for consistency
            for (ServerConfig server : allServers) {
                String serverId = server.getServerId();

                // Skip self
                if (serverId.equals(currentServerId)) {
                    continue;
                }

                try {
                    // Ask the server what files it's missing (comparison against ZK metadata)
                    // If it's missing files, we replicate them.
                    syncServerFiles(serverId);
                } catch (Exception e) {
                    // Only log if it's a real failure, not just a dead server
                    logger.debug("Consistency check failed for server {}: {}", serverId, e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.debug("Error in proactive sync monitor: {}", e.getMessage());
        }
    }

    /**
     * Fetch missing files for a server and replicate them
     */
    private void syncServerFiles(String serverId) throws Exception {
        logger.info("Starting sync for server: {}", serverId);

        // Get server details
        var serverDetails = zooKeeperService.getAllServerDetails();
        String serverAddress = serverDetails.get(serverId);

        if (serverAddress == null) {
            logger.warn("Server address not found for: {}", serverId);
            return;
        }

        String serverUrl = "http://" + serverAddress;

        try {
            // Get leader server
            String leaderId = zooKeeperService.getLeader();
            var leaderDetails = zooKeeperService.getAllServerDetails();
            String leaderAddress = leaderDetails.get(leaderId);
            String leaderUrl = "http://" + leaderAddress;

            // Step 1: Get list of missing files for this server
            List<String> missingFiles = getMissingFilesForServer(serverUrl);

            if (missingFiles.isEmpty()) {
                logger.info("Server {} has all files - sync complete", serverId);
                markSyncComplete(serverId);
                return;
            }

            logger.info("Server {} is missing {} files, starting replication...", serverId, missingFiles.size());

            // Step 2: Replicate each missing file from leader
            int successCount = 0;
            for (String filename : missingFiles) {
                try {
                    // Get file from leader
                    byte[] fileData = getFileFromLeader(leaderUrl, filename);
                    if (fileData != null && fileData.length > 0) {
                        // Replicate to the server
                        ReplicaClient.replicateFile(serverUrl, filename, fileData);
                        successCount++;
                        logger.debug("Replicated file {} to server {}", filename, serverId);
                    } else {
                        logger.warn("Could not retrieve file {} from leader", filename);
                    }
                } catch (Exception e) {
                    logger.error("Failed to replicate file {} to server {}: {}", filename, serverId, e.getMessage());
                }
            }

            logger.info("Replication complete for server {}: {} / {} files", serverId, successCount,
                    missingFiles.size());

            // Step 3: Mark sync as complete
            markSyncComplete(serverId);

        } catch (Exception e) {
            logger.error("Error during sync for server {}: {}", serverId, e.getMessage());
            throw e;
        }
    }

    /**
     * Get file from leader server
     */
    private byte[] getFileFromLeader(String leaderUrl, String filename) throws Exception {
        String encodedFilename = java.net.URLEncoder.encode(filename, java.nio.charset.StandardCharsets.UTF_8);
        String url = leaderUrl + "/download?file=" + encodedFilename;
        HttpURLConnection connection = null;

        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                byte[] fileData = connection.getInputStream().readAllBytes();
                logger.debug("Retrieved file {} from leader (size: {} bytes)", filename, fileData.length);
                return fileData;
            } else {
                logger.warn("Failed to get file {} from leader: HTTP {}", filename, responseCode);
                return null;
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Get list of missing files for a specific server
     */
    @SuppressWarnings("unchecked")
    private List<String> getMissingFilesForServer(String serverUrl) throws Exception {
        String url = serverUrl + "/get-missing-files";
        HttpURLConnection connection = null;

        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new Exception("Server returned code: " + responseCode);
            }

            // Parse response JSON
            byte[] responseBytes = connection.getInputStream().readAllBytes();
            Map<String, Object> response = objectMapper.readValue(responseBytes, Map.class);

            List<String> missingFiles = (List<String>) response.get("missingFiles");
            return missingFiles != null ? missingFiles : List.of();

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Mark sync as complete for a server by calling its /sync-complete endpoint
     */
    private void markSyncComplete(String serverId) {
        try {
            var serverDetails = zooKeeperService.getAllServerDetails();
            String serverAddress = serverDetails.get(serverId);

            if (serverAddress == null) {
                logger.warn("Server address not found for: {}", serverId);
                return;
            }

            String url = "http://" + serverAddress + "/sync-complete";
            HttpURLConnection connection = null;

            try {
                connection = (HttpURLConnection) new URL(url).openConnection();
                connection.setRequestMethod("POST");
                connection.setConnectTimeout(TIMEOUT_MS);
                connection.setReadTimeout(TIMEOUT_MS);

                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    logger.info("✅ Marked sync complete for server: {}", serverId);
                } else {
                    logger.warn("Failed to mark sync complete for {}: HTTP {}", serverId, responseCode);
                }
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        } catch (Exception e) {
            logger.error("Error marking sync complete for server {}: {}", serverId, e.getMessage());
        }
    }

    /**
     * Shutdown sync service
     */
    public void shutdown() {
        if (syncExecutor != null) {
            syncExecutor.shutdown();
            try {
                if (!syncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    syncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                syncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Server sync service shut down");
        }
    }
}
