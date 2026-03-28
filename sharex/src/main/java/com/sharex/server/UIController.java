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
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;

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
     * Serve the web UI index.html at the root path
     * GET /
     */
    @GetMapping("/")
    public ResponseEntity<?> serveIndex() {
        try {
            // Get the classpath resource for index.html
            String indexPath = "classpath:static/index.html";
            String indexContent = new String(Files.readAllBytes(
                    Paths.get(getClass().getClassLoader().getResource("static/index.html").toURI())
            ));
            return ResponseEntity.ok()
                    .header("Content-Type", "text/html; charset=UTF-8")
                    .body(indexContent);
        } catch (Exception e) {
            logger.error("Failed to serve index.html", e);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("<html><body><h1>Error loading UI</h1><p>" + e.getMessage() + "</p></body></html>");
        }
    }

    /**
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
            
            // Get files from current server
            String storagePath = clusterConfig.getCurrentServer().getStoragePath();
            File storageDir = new File(storagePath);
            
            if (storageDir.exists() && storageDir.isDirectory()) {
                File[] fileArray = storageDir.listFiles();
                if (fileArray != null) {
                    for (File file : fileArray) {
                        if (file.isFile()) {
                            allFileNames.add(file.getName());
                        }
                    }
                }
            }
            
            // Get files from all other servers using the internal endpoint
            String currentServerId = clusterConfig.getCurrentServer().getServerId();
            for (ServerConfig server : clusterConfig.getAllServers()) {
                if (!server.getServerId().equals(currentServerId)) {
                    try {
                        String filesEndpoint = server.getBaseUrl() + "/internal/files";
                        HttpURLConnection connection = (HttpURLConnection) new java.net.URL(filesEndpoint).openConnection();
                        connection.setRequestMethod("GET");
                        connection.setConnectTimeout(10000);
                        connection.setReadTimeout(10000);
                        
                        if (connection.getResponseCode() == 200) {
                            String response = new String(connection.getInputStream().readAllBytes());
                            @SuppressWarnings("unchecked")
                            Map<String, Object> jsonMap = new ObjectMapper()
                                    .readValue(response, java.util.Map.class);
                            
                            @SuppressWarnings("unchecked")
                            List<String> otherServerFiles = (List<String>) jsonMap.get("files");
                            if (otherServerFiles != null) {
                                allFileNames.addAll(otherServerFiles);
                            }
                        }
                        connection.disconnect();
                    } catch (java.net.SocketTimeoutException e) {
                        logger.warn("Timeout getting files from server {} - continuing anyway", server.getServerId());
                    } catch (Exception e) {
                        logger.warn("Could not reach server {} - continuing with available files", server.getServerId());
                    }
                }
            }
            
            List<String> sortedFiles = new ArrayList<>(allFileNames);
            sortedFiles.sort(String::compareTo);
            
            logger.info("Listed {} unique files", sortedFiles.size());
            
            return ResponseEntity.ok(Map.of(
                    "files", sortedFiles,
                    "totalFiles", sortedFiles.size()
            ));
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

            logger.info("File {} deleted from all servers", filename);
            return ResponseEntity.ok(Map.of(
                    "message", "File deleted from all servers",
                    "filename", filename,
                    "server", clusterConfig.getCurrentServer().getServerId()
            ));

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
                    "isCurrentServerLeader", clusterConfig.isCurrentServerLeader()
            ));
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
                    "leader", clusterConfig.getLeader().getServerId(),
                    "zkConnected", zooKeeperService.isConnected(),
                    "allServers", clusterConfig.getAllServers().stream()
                            .map(s -> Map.of(
                                    "id", s.getServerId(),
                                    "port", s.getPort(),
                                    "url", s.getBaseUrl()
                            ))
                            .collect(Collectors.toList())
            ));
        } catch (Exception e) {
            logger.error("Failed to get system info", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to get info"));
        }
    }

    /**
     * Helper: Format file size for display
     */
    private String formatFileSize(long size) {
        if (size <= 0) return "0 B";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return String.format("%.1f %s", size / Math.pow(1024, digitGroups), units[digitGroups]);
    }
}
