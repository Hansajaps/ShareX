package com.sharex.server;

import java.io.InputStream;

/**
 * Interface for file operations (upload, download, delete).
 * Implementations handle local storage operations.
 */
public interface FileService {
    /**
     * Upload/store a file on the local filesystem.
     */
    void uploadFile(String filename, InputStream fileData) throws Exception;

    /**
     * Download/retrieve a file from local filesystem.
     */
    byte[] downloadFile(String filename) throws Exception;

    /**
     * Check if a file exists on the local filesystem.
     */
    boolean fileExists(String filename);

    /**
     * Delete a file from local filesystem.
     */
    void deleteFile(String filename) throws Exception;

    /**
     * Get the absolute path to a file.
     */
    String getFilePath(String filename);
}
