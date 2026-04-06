package com.sharex.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.sharex.replication.ClusterConfig;
import com.sharex.replication.ReplicaClient;
import com.sharex.replication.ServerConfig;
import com.sharex.zookeeper.ZooKeeperService;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * UI Controller for web interface
 * Provides endpoints for the web UI dashboard
 */
@RestController
@CrossOrigin(origins = "*")
public class UIController {
    private static final Logger logger = LoggerFactory.getLogger(UIController.class);

    @Autowired
    private FileService fileService;

    @Autowired
    private ClusterConfig clusterConfig;

    @Autowired
    private ZooKeeperService zooKeeperService;


    /**
     * Serve the dashboard index.html directly from the classpath to avoid 404s
     */
    @GetMapping({"/", "/index.html"})
    @ResponseBody
    public ResponseEntity<String> serveIndex() {
        try {
            org.springframework.core.io.ClassPathResource resource = new org.springframework.core.io.ClassPathResource("static/index.html");
            byte[] bdata = org.springframework.util.FileCopyUtils.copyToByteArray(resource.getInputStream());
            String data = new String(bdata, java.nio.charset.StandardCharsets.UTF_8);
            return ResponseEntity.ok()
                    .header(org.springframework.http.HttpHeaders.CONTENT_TYPE, "text/html")
                    .body(data);
        } catch (java.io.IOException e) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.NOT_FOUND)
                    .body("<h1>Error: Could not load index.html from classpath</h1><p>" + e.getMessage() + "</p>");
        }
    }

    /**
     * Internal endpoint - Get only files from current server (no recursion)
     * Internal endpoint - Get only files from current server (no recursion)
     * GET /internal/files
     */
    @GetMapping("/internal/files")
    public ResponseEntity<?> getLocalFiles() {
        try {
            List<String> fileNames = new ArrayList<>();
            String storagePath = clusterConfig.getCurrentServer().getStoragePath();
            File storageDir = new File(storagePath);

            if (storageDir.exists() && storageDir.isDirectory()) {
                File[] fileArray = storageDir.listFiles();
                if (fileArray != null) {
                    for (File file : fileArray) {
                        if (file.isFile()) {
                            fileNames.add(file.getName());
                        }
                    }
                }
            }

            fileNames.sort(String::compareTo);
            return ResponseEntity.ok(Map.of("files", fileNames));
        } catch (Exception e) {
            logger.error("Failed to get local files", e);
            return ResponseEntity.ok(Map.of("files", new ArrayList<>()));
        }
    }

    /**
     * Get list of all files from all servers in the cluster
     * GET /files
     */
    @GetMapping("/files")
    public ResponseEntity<?> listFiles() {
        try {
            Set<String> allFileNames = new LinkedHashSet<>();

            // source 1: ZooKeeper metadata (Global view - SOURCE OF TRUTH)
            if (zooKeeperService != null && zooKeeperService.isConnected()) {
                List<String> zkFiles = zooKeeperService.getRegisteredFiles();
                if (zkFiles != null) {
                    allFileNames.addAll(zkFiles);
                    logger.info("Listed {} files from ZooKeeper (Source of Truth)", zkFiles.size());
                }
            } else {
                // FALLBACK ONLY: If ZooKeeper is down, read from disk
                logger.warn("ZooKeeper disconnected! Falling back to local disk listing.");
                String storagePath = clusterConfig.getCurrentServer().getStoragePath();
                File storageDir = new File(storagePath);
                if (storageDir.exists() && storageDir.isDirectory()) {
                    File[] fileArray = storageDir.listFiles();
                    if (fileArray != null) {
                        for (File file : fileArray) {
                            if (file.isFile()) allFileNames.add(file.getName());
                        }
                    }
                }
            }

            List<String> sortedFiles = new ArrayList<>(allFileNames);
            sortedFiles.sort(String::compareTo);

            return ResponseEntity.ok(Map.of(
                    "files", sortedFiles,
                    "totalFiles", sortedFiles.size()));
        } catch (Exception e) {
            logger.error("Failed to list files", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to list files: " + e.getMessage()));
        }
    }

    /**
     * Delete a file from all servers
     * DELETE /delete?file=filename
     * 
     * Only leader can delete (to maintain consistency)
     */
    @DeleteMapping("/delete")
    public ResponseEntity<?> deleteFile(@RequestParam("file") String filename) {
        logger.info("Delete request for file: {}", filename);

        // Only leader can delete
        if (!clusterConfig.isCurrentServerLeader()) {
            logger.warn("Delete rejected: Server {} is not the leader", clusterConfig.getCurrentServer().getServerId());
            return ResponseEntity
                    .status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Only the leader can delete files"));
        }

        try {
            // Delete on leader
            fileService.deleteFile(filename);
            logger.info("File deleted from leader: {}", filename);

            // Delete on all followers
            for (var follower : clusterConfig.getFollowers()) {
                try {
                    logger.info("Deleting file {} from follower: {}", filename, follower.getServerId());
                    ReplicaClient.deleteFileReplica(follower.getBaseUrl(), filename);
                } catch (Exception e) {
                    logger.error("Failed to delete from {}: {}", follower.getServerId(), e.getMessage());
                }
            }

            // Step 3: Remove from ZooKeeper metadata (CRITICAL FIX)
            if (zooKeeperService != null && zooKeeperService.isConnected()) {
                logger.info("Unregistering file {} from ZooKeeper", filename);
                zooKeeperService.unregisterFileMetadata(filename);
            }

            logger.info("File {} deleted from all servers and ZooKeeper", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File deleted from all servers",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()));

        } catch (Exception e) {
            logger.error("Delete failed: {}", e.getMessage(), e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Delete failed: " + e.getMessage()));
        }
    }

    /**
     * Get current leader information
     * GET /api/leader
     * 
     * Clients can use this endpoint to discover the current leader
     */
    @GetMapping("/api/leader")
    public ResponseEntity<?> getLeaderInfo() {
        try {
            ServerConfig leader = clusterConfig.getLeader();
            return ResponseEntity.ok(Map.of(
                    "leaderId", leader.getServerId(),
                    "leaderUrl", leader.getBaseUrl(),
                    "currentServer", clusterConfig.getCurrentServer().getServerId(),
                    "isCurrentServerLeader", clusterConfig.isCurrentServerLeader(),
                    "allServers", clusterConfig.getAllServers().stream()
                            .map(s -> Map.of(
                                    "id", s.getServerId(),
                                    "port", s.getPort(),
                                    "url", s.getBaseUrl()))
                            .collect(Collectors.toList())));
        } catch (Exception e) {
            logger.error("Failed to get leader info", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get leader info"));
        }
    }

    /**
     * Get system info
     * GET /api/info
     */
    @GetMapping("/api/info")
    public ResponseEntity<?> getSystemInfo() {
        try {
            return ResponseEntity.ok(Map.of(
                    "currentServer", clusterConfig.getCurrentServer().getServerId(),
                    "isLeader", clusterConfig.isCurrentServerLeader(),
                    "leader", clusterConfig.getLeader() != null ? clusterConfig.getLeader().getServerId() : "electing...",
                    "zkConnected", zooKeeperService.isConnected(),
                    "allServers", clusterConfig.getAllServers().stream()
                            .map(s -> Map.of(
                                    "id", s.getServerId(),
                                    "port", s.getPort(),
                                    "url", s.getBaseUrl()))
                            .collect(Collectors.toList())));
        } catch (Exception e) {
            logger.error("Failed to get system info", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get info"));
        }
    }

    /**
     * Delete ALL files from ALL servers and wipe metadata (FOR TESTING/CLEANUP)
     * DELETE /purge
     */
    @DeleteMapping("/purge")
    public ResponseEntity<?> purgeAll() {
        logger.info("CRITICAL: Purge request for ALL cluster data");

        if (!clusterConfig.isCurrentServerLeader()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "Only the leader can purge the cluster"));
        }

        try {
            // Get all files from ZK first (so we know what to wipe on followers)
            List<String> allFiles = zooKeeperService.getRegisteredFiles();
            
            // Delete local files on leader
            for (String filename : allFiles) {
                try { fileService.deleteFile(filename); } catch (Exception ignored) {}
                zooKeeperService.unregisterFileMetadata(filename);
            }

            // Tell all followers to purge
            for (var follower : clusterConfig.getFollowers()) {
                try {
                    // We can reuse delete replica or add a new purge replica endpoint
                    for (String filename : allFiles) {
                        ReplicaClient.deleteFileReplica(follower.getBaseUrl(), filename);
                    }
                } catch (Exception e) {
                    logger.error("Failed to purge follow {}: {}", follower.getServerId(), e.getMessage());
                }
            }

            return ResponseEntity.ok(Map.of("message", "System purge complete. All data wiped."));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", "Purge failed: " + e.getMessage()));
        }
    }
}
