package com.sharex.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
// removed unused imports

/**
 * HTTP Client for replicating files to follower/backup servers.
 * Used by the leader server to push file updates to all replicas.
 */
public class ReplicaClient {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaClient.class);
    private static final int TIMEOUT_MS = 10000; // 10 second timeout

    /**
     * Send a file to a remote server via HTTP POST.
     * This is called by the leader to replicate files to followers.
     * 
     * @param targetServerUrl Base URL of the target server (e.g., http://localhost:8082)
     * @param filename File name to replicate
     * @param fileData File content as byte array
     * @throws Exception if replication fails
     */
    public static void replicateFile(String targetServerUrl, String filename, byte[] fileData) throws Exception {
        String encodedFilename = java.net.URLEncoder.encode(filename, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
        String replicaUrl = targetServerUrl + "/replicate?file=" + encodedFilename;
        logger.info("Replicating file {} to {}", filename, targetServerUrl);
        
        HttpURLConnection connection = null;
        try {
            URL url = new URL(replicaUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/octet-stream");
            connection.setRequestProperty("Content-Length", String.valueOf(fileData.length));
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);

            // Send file data
            try (OutputStream outStream = connection.getOutputStream()) {
                outStream.write(fileData);
                outStream.flush();
            }

            // Check response
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                String errorMsg = readErrorResponse(connection);
                logger.error("Replication failed: {} returned code {}: {}", targetServerUrl, responseCode, errorMsg);
                throw new Exception("Replication failed: HTTP " + responseCode);
            }

            logger.info("File {} replicated successfully to {}", filename, targetServerUrl);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Send file deletion request to a remote server via HTTP DELETE.
     */
    public static void deleteFileReplica(String targetServerUrl, String filename) throws Exception {
        String encodedFilename = java.net.URLEncoder.encode(filename, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
        String replicaUrl = targetServerUrl + "/replicate?file=" + encodedFilename;
        logger.info("Deleting replica of {} from {}", filename, targetServerUrl);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(replicaUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            connection.setConnectTimeout(TIMEOUT_MS);
            connection.setReadTimeout(TIMEOUT_MS);

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                String errorMsg = readErrorResponse(connection);
                logger.error("Deletion replication failed: {} returned code {}: {}", targetServerUrl, responseCode, errorMsg);
                throw new Exception("Deletion replication failed: HTTP " + responseCode);
            }

            logger.info("File {} deleted from replica: {}", filename, targetServerUrl);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Helper method to read error response from connection.
     */
    private static String readErrorResponse(HttpURLConnection connection) {
        try (InputStream errorStream = connection.getErrorStream()) {
            if (errorStream != null) {
                return new String(errorStream.readAllBytes());
            }
        } catch (IOException e) {
            logger.debug("Could not read error response", e);
        }
        return "";
    }
}
