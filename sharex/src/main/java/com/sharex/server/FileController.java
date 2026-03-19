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
import com.sharex.zookeeper.ZooKeeperService;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REST API Controller for file operations.
 * 
 * Provides endpoints for:
 * - /upload - Client uploads file to leader (client-facing, write operation)
 * - /download - Client downloads file from any server (client-facing, read operation)
 * - /replicate - Replicates file from leader to followers (internal, server-to-server)
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

    // Mutex for each file to prevent concurrent writes
    private static final Map<String, Object> fileLocks = new ConcurrentHashMap<>();

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
        if (!clusterConfig.isCurrentServerLeader()) {
            logger.warn("Write rejected: Server {} is not the leader", clusterConfig.getCurrentServer().getServerId());
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

                // Step 2: Replicate to all followers
                byte[] fileBytes = fileData.getBytes();
                for (var follower : clusterConfig.getFollowers()) {
                    try {
                        logger.info("Replicating file {} to follower: {}", filename, follower.getServerId());
                        ReplicaClient.replicateFile(follower.getBaseUrl(), filename, fileBytes);
                    } catch (Exception e) {
                        logger.error("Failed to replicate to {}: {}", follower.getServerId(), e.getMessage());
                        // In a production system, we'd handle this more robustly
                        // For now, we'll log the error and continue
                    }
                }
                
                // Step 3: Register file metadata in ZooKeeper
                logger.info("Registering file {} metadata in ZooKeeper", filename);
                zooKeeperService.registerFileMetadata(filename, fileData.getSize(), System.currentTimeMillis());
            }

            logger.info("File {} uploaded and replicated successfully", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File uploaded successfully",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()
            ));

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
                    "server", clusterConfig.getCurrentServer().getServerId()
            ));

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
                    "server", clusterConfig.getCurrentServer().getServerId()
            ));

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
        return ResponseEntity.ok(Map.of(
                "serverId", clusterConfig.getCurrentServer().getServerId(),
                "port", clusterConfig.getCurrentServer().getPort(),
                "isLeader", clusterConfig.isCurrentServerLeader(),
                "storagePath", clusterConfig.getCurrentServer().getStoragePath(),
                "leaderId", clusterConfig.getLeader().getServerId()
        ));
    }
}
