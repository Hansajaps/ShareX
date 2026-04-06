package com.sharex.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import com.sharex.replication.ClusterConfig;
import com.sharex.replication.ReplicaClient;
import com.sharex.replication.ServerConfig;
import com.sharex.zookeeper.ZooKeeperService;
import com.sharex.time.TimeSyncService;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * REST API Controller for file operations.
 * 
 * Provides endpoints for:
 * - /upload - Client uploads file to leader (client-facing, write operation)
 * - /download - Client downloads file from any server (client-facing, read
 * operation)
 * - /replicate - Replicates file from leader to followers (internal,
 * server-to-server)
 * 
 * Consistency Model:
 * - Only the leader accepts direct client write requests (/upload)
 * - Followers reject client writes with 403 Forbidden
 * - Followers accept replication requests from leader via /replicate
 * - Concurrent writes are serialized at the leader using a mutex per file
 */
@RestController
public class FileController {
    private static final Logger logger = LoggerFactory.getLogger(FileController.class);

    @Autowired
    private FileService fileService;

    @Autowired
    private ClusterConfig clusterConfig;

    @Autowired
    private ZooKeeperService zooKeeperService;

    @Autowired
    private TimeSyncService timeSyncService;

    @Autowired
    private com.sharex.consensus.RaftNode raftNode;

    // Mutex for each file to prevent concurrent writes
    private static final Map<String, Object> fileLocks = new ConcurrentHashMap<>();

    // Thread pool for asynchronous replication with retry logic
    private final ScheduledExecutorService replicationExecutor = Executors.newScheduledThreadPool(5);
    private static final int MAX_REPLICATION_RETRIES = 3;

    /**
     * CLIENT-FACING: Upload endpoint
     * POST /upload?file=filename
     * 
     * Accepts file uploads only on the LEADER server.
     * Followers reject writes with 403 Forbidden.
     * 
     * Flow:
     * 1. Check if current server is leader
     * 2. If not leader, reject (403 Forbidden)
     * 3. If leader, acquire lock for this file
     * 4. Save file locally
     * 5. Replicate to all followers in parallel
     * 6. Return success if all replications succeed
     */
    @PostMapping("/upload")
    public ResponseEntity<?> uploadFile(
            @RequestParam("file") String filename,
            @RequestParam("data") MultipartFile fileData) {

        logger.info("Upload request received for file: {}", filename);

        // CONSISTENCY CHECK: Only leader accepts writes
        // Check both ClusterConfig (ZooKeeper) and RaftNode for leadership
        boolean isLeader = clusterConfig.isCurrentServerLeader() || raftNode.getState().isLeader();
        
        if (!isLeader) {
            logger.warn("Write rejected: Server {} is not the leader (Raft: {}, ZK: {})", 
                        clusterConfig.getCurrentServer().getServerId(),
                        raftNode.getState().isLeader(),
                        clusterConfig.isCurrentServerLeader());
            return ResponseEntity
                    .status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "This server is a follower. Only the leader accepts writes."));
        }

        try {
            // Serialize writes to the same file (prevent concurrent writes)
            Object fileLock = fileLocks.computeIfAbsent(filename, k -> new Object());
            synchronized (fileLock) {
                // Step 1: Save file locally on leader
                logger.info("Saving file locally on leader: {}", filename);
                fileService.uploadFile(filename, fileData.getInputStream());

                // Step 2: Replicate to ALL other servers (except self) ASYNCHRONOUSLY
                byte[] fileBytes = fileData.getBytes();
                List<ServerConfig> allServers = clusterConfig.getAllServers();
                String currentServerId = clusterConfig.getCurrentServer().getServerId();

                for (ServerConfig server : allServers) {
                    // Skip replicating to self
                    if (server.getServerId().equals(currentServerId)) {
                        logger.debug("Skipping self ({}) in replication", currentServerId);
                        continue;
                    }

                    // Submit async replication task with retry logic
                    replicationExecutor.submit(() -> {
                        replicateWithRetry(filename, server, fileBytes, 0);
                    });
                }

                // Step 3: Log operation in Raft consensus log (Member 4 responsibility)
                logger.info("Submitting UPLOAD command to Raft consensus log for {}", filename);
                raftNode.submitCommand("UPLOAD:" + filename, "size=" + fileData.getSize());

                // Step 4: Register file metadata in ZooKeeper
                logger.info("Registering file {} metadata in ZooKeeper", filename);
                zooKeeperService.registerFileMetadata(filename, fileData.getSize(), timeSyncService.getCurrentTime());
            }

            logger.info("File {} uploaded and replicated successfully", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File uploaded successfully",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()));

        } catch (Exception e) {
            logger.error("Upload failed: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Upload failed: " + e.getMessage()));
        }
    }

    /**
     * CLIENT-FACING: Download endpoint
     * GET /download?file=filename
     * 
     * Allows downloading from any server (leader or follower).
     * This is a read operation and doesn't violate consistency.
     */
    @GetMapping("/download")
    public ResponseEntity<?> downloadFile(@RequestParam("file") String filename) {
        logger.info("Download request received for file: {}", filename);

        try {
            if (!fileService.fileExists(filename)) {
                logger.warn("File not found: {}", filename);
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "File not found: " + filename));
            }

            byte[] fileBytes = fileService.downloadFile(filename);
            logger.info("File {} downloaded successfully (size: {} bytes)", filename, fileBytes.length);
            return ResponseEntity.ok()
                    .header("Content-Disposition", "attachment; filename=\"" + filename + "\"")
                    .header("X-Server", clusterConfig.getCurrentServer().getServerId())
                    .body(fileBytes);

        } catch (FileNotFoundException e) {
            logger.warn("File not found: {}", filename);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "File not found: " + filename));
        } catch (Exception e) {
            logger.error("Download failed: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Download failed: " + e.getMessage()));
        }
    }

    /**
     * INTERNAL: Replication endpoint
     * POST /replicate?file=filename
     * 
     * Receives files from the leader for replication.
     * Only used for server-to-server communication.
     * Followers accept replication requests and save the file.
     */
    @PostMapping("/replicate")
    public ResponseEntity<?> replicateFile(
            @RequestParam("file") String filename,
            @RequestBody byte[] fileData) {

        logger.info("Replication request received for file: {} (size: {} bytes)", filename, fileData.length);

        try {
            // Serialize writes using file lock
            Object fileLock = fileLocks.computeIfAbsent(filename, k -> new Object());
            synchronized (fileLock) {
                // Save the replicated file
                fileService.uploadFile(filename, new java.io.ByteArrayInputStream(fileData));
            }

            logger.info("File {} replicated and saved successfully", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File replicated successfully",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()));

        } catch (Exception e) {
            logger.error("Replication failed: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Replication failed: " + e.getMessage()));
        }
    }

    /**
     * INTERNAL: Delete replication endpoint
     * DELETE /replicate?file=filename
     * 
     * Receives delete commands from the leader.
     * Followers delete the file to maintain consistency.
     */
    @DeleteMapping("/replicate")
    public ResponseEntity<?> deleteFileReplica(@RequestParam("file") String filename) {
        logger.info("Delete replication request received for file: {}", filename);

        try {
            Object fileLock = fileLocks.computeIfAbsent(filename, k -> new Object());
            synchronized (fileLock) {
                fileService.deleteFile(filename);
            }

            logger.info("File {} deleted successfully on follower", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File deleted successfully",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()));

        } catch (Exception e) {
            logger.error("Delete replication failed: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Delete replication failed: " + e.getMessage()));
        }
    }

    /**
     * UTILITY: Server status endpoint
     * GET /status
     * 
     * Returns information about this server's state.
     * Useful for debugging and verification.
     */
    @GetMapping("/status")
    public ResponseEntity<?> serverStatus() {
        if (zooKeeperService.isSimulatedCrash()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        String raftLeaderId = raftNode.getState().getLeaderId();
        ServerConfig zkLeaderConfig = clusterConfig.getLeader();
        // Safe fallback: never NPE when no leader has been elected yet
        String displayLeaderId = (raftLeaderId != null) ? raftLeaderId
                : (zkLeaderConfig != null ? zkLeaderConfig.getServerId() : "electing...");

        return ResponseEntity.ok(Map.of(
                "serverId", clusterConfig.getCurrentServer().getServerId(),
                "port", clusterConfig.getCurrentServer().getPort(),
                "isLeader", raftNode.getState().isLeader() || clusterConfig.isCurrentServerLeader(),
                "raftIsLeader", raftNode.getState().isLeader(),
                "zkIsLeader", clusterConfig.isCurrentServerLeader(),
                "zkConnected", zooKeeperService.isConnected(),
                "storagePath", clusterConfig.getCurrentServer().getStoragePath(),
                "leaderId", displayLeaderId,
                "raftState", raftNode.getState().getState().toString(),
                "raftTerm", raftNode.getState().getCurrentTerm()));
    }

    /**
     * INTERNAL: List all files in cluster
     * GET /list-files
     * 
     * Returns list of all files that exist in the cluster (from ZooKeeper
     * metadata).
     * Used by the leader to identify which files need to be replicated to other
     * servers.
     */
    @GetMapping("/list-files")
    public ResponseEntity<?> listAllFiles() {
        logger.info("List files request received from: {}", clusterConfig.getCurrentServer().getServerId());

        try {
            List<String> allFiles = zooKeeperService.getRegisteredFiles();
            logger.info("Returning {} files from cluster metadata", allFiles.size());
            return ResponseEntity.ok(Map.of(
                    "files", allFiles,
                    "server", clusterConfig.getCurrentServer().getServerId()));
        } catch (Exception e) {
            logger.error("Failed to list files: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to list files: " + e.getMessage()));
        }
    }

    /**
     * INTERNAL: Get list of missing files for this server
     * GET /get-missing-files
     * 
     * Returns list of files that exist in the cluster but are missing on this
     * server.
     * The leader uses this to determine which files need to be replicated to this
     * server.
     * Useful for server consistency when a server comes back online.
     */
    @GetMapping("/get-missing-files")
    public ResponseEntity<?> getMissingFiles() {
        logger.info("Get missing files request received for server: {}",
                clusterConfig.getCurrentServer().getServerId());

        try {
            List<String> allClusterFiles = zooKeeperService.getRegisteredFiles();
            List<String> missingFiles = new ArrayList<>();

            // Check which files are missing locally
            for (String filename : allClusterFiles) {
                if (!fileService.fileExists(filename)) {
                    missingFiles.add(filename);
                }
            }

            logger.info("Server {} is missing {} out of {} files",
                    clusterConfig.getCurrentServer().getServerId(),
                    missingFiles.size(),
                    allClusterFiles.size());

            return ResponseEntity.ok(Map.of(
                    "missingFiles", missingFiles,
                    "totalClusterFiles", allClusterFiles.size(),
                    "server", clusterConfig.getCurrentServer().getServerId()));
        } catch (Exception e) {
            logger.error("Failed to get missing files: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get missing files: " + e.getMessage()));
        }
    }

    /**
     * INTERNAL: Sync endpoint for when server comes online
     * POST /sync-complete
     * 
     * Called by the client side (Application/startup) after all missing files
     * have been replicated to indicate that sync is complete.
     * This removes the server from the sync registry in ZooKeeper.
     */
    @PostMapping("/sync-complete")
    public ResponseEntity<?> syncComplete() {
        logger.info("Sync complete notification received for server: {}",
                clusterConfig.getCurrentServer().getServerId());

        try {
            // Remove this server from the sync registry
            zooKeeperService.unregisterServerFromSync(clusterConfig.getCurrentServer().getServerId());

            logger.info("✅ Server {} sync complete and registered as ready",
                    clusterConfig.getCurrentServer().getServerId());
            return ResponseEntity.ok(Map.of(
                    "message", "Sync complete",
                    "server", clusterConfig.getCurrentServer().getServerId()));
        } catch (Exception e) {
            logger.error("Failed to mark sync complete: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to mark sync complete: " + e.getMessage()));
        }
    }

    /**
     * INTERNAL: Async replication with retry logic
     * Attempts to replicate a file to a target server with exponential backoff
     * retry.
     * This ensures that transient network issues don't prevent replication
     * immediately.
     * 
     * @param filename  File to replicate
     * @param server    Target server
     * @param fileBytes File content
     * @param attempt   Current attempt number
     */
    private void replicateWithRetry(String filename, ServerConfig server, byte[] fileBytes, int attempt) {
        try {
            logger.info("Replicating file {} to server: {} (attempt {}/{})",
                    filename, server.getServerId(), attempt + 1, MAX_REPLICATION_RETRIES);
            ReplicaClient.replicateFile(server.getBaseUrl(), filename, fileBytes);
            logger.info("✅ Successfully replicated {} to {}", filename, server.getServerId());
        } catch (Exception e) {
            if (attempt < MAX_REPLICATION_RETRIES - 1) {
                // Retry with exponential backoff: 1s, 2s, 4s
                long delayMs = (long) (1000 * Math.pow(2, attempt));
                logger.warn("Replication of {} to {} failed (attempt {}/{}), retrying in {}ms: {}",
                        filename, server.getServerId(), attempt + 1, MAX_REPLICATION_RETRIES, delayMs, e.getMessage());

                // Schedule retry
                replicationExecutor.schedule(() -> {
                    replicateWithRetry(filename, server, fileBytes, attempt + 1);
                }, delayMs, TimeUnit.MILLISECONDS);
            } else {
                logger.warn("Failed to replicate {} to {} after {} attempts (will sync when online): {}",
                        filename, server.getServerId(), MAX_REPLICATION_RETRIES, e.getMessage());
                // Server will sync this file when it comes online via ServerSyncService
            }
        }
    }

    @PostMapping("/api/admin/crash")
    public ResponseEntity<?> crashServer() {
        zooKeeperService.simulateCrash();
        return ResponseEntity.ok(Map.of("message", "Server " + clusterConfig.getCurrentServer().getServerId() + " successfully crashed."));
    }

    @PostMapping("/api/admin/recover")
    public ResponseEntity<?> recoverServer() {
        zooKeeperService.recoverFromCrash();
        return ResponseEntity.ok(Map.of("message", "Server " + clusterConfig.getCurrentServer().getServerId() + " successfully recovered."));
    }
}
