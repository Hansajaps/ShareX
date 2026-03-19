package com.sharex.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.file.*;

/**
 * Concrete implementation of FileService.
 * Handles all local file storage operations on the filesystem.
 */
public class FileServiceImpl implements FileService {
    private static final Logger logger = LoggerFactory.getLogger(FileServiceImpl.class);
    private final String storagePath;

    public FileServiceImpl(String storagePath) {
        this.storagePath = storagePath;
        // Create storage directory if it doesn't exist
        try {
            Path path = Paths.get(storagePath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                logger.info("Created storage directory: {}", storagePath);
            }
        } catch (IOException e) {
            logger.error("Failed to create storage directory: {}", storagePath, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Upload a file to local storage.
     * Reads all bytes from the input stream and writes to file.
     */
    @Override
    public void uploadFile(String filename, InputStream fileData) throws Exception {
        logger.debug("Uploading file: {} to {}", filename, storagePath);
        Path filePath = Paths.get(storagePath, filename);
        
        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());
        
        // Write file data
        try (OutputStream outStream = Files.newOutputStream(filePath)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fileData.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
        }
        logger.info("File uploaded successfully: {}", filePath);
    }

    /**
     * Download a file from local storage.
     * Returns the entire file as a byte array.
     */
    @Override
    public byte[] downloadFile(String filename) throws Exception {
        logger.debug("Downloading file: {} from {}", filename, storagePath);
        Path filePath = Paths.get(storagePath, filename);
        
        if (!Files.exists(filePath)) {
            logger.warn("File not found: {}", filePath);
            throw new FileNotFoundException("File not found: " + filename);
        }
        
        byte[] data = Files.readAllBytes(filePath);
        logger.info("File downloaded successfully: {} (size: {} bytes)", filePath, data.length);
        return data;
    }

    /**
     * Check if a file exists in local storage.
     */
    @Override
    public boolean fileExists(String filename) {
        Path filePath = Paths.get(storagePath, filename);
        boolean exists = Files.exists(filePath);
        logger.debug("File exists check: {} -> {}", filename, exists);
        return exists;
    }

    /**
     * Delete a file from local storage.
     */
    @Override
    public void deleteFile(String filename) throws Exception {
        logger.debug("Deleting file: {}", filename);
        Path filePath = Paths.get(storagePath, filename);
        
        if (!Files.exists(filePath)) {
            logger.warn("File not found for deletion: {}", filePath);
            throw new FileNotFoundException("File not found: " + filename);
        }
        
        Files.delete(filePath);
        logger.info("File deleted successfully: {}", filePath);
    }

    /**
     * Get the absolute file path (useful for logging/debugging).
     */
    @Override
    public String getFilePath(String filename) {
        return Paths.get(storagePath, filename).toAbsolutePath().toString();
    }
}
