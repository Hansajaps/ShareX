package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * State machine that applies committed Raft log entries.
 * For ShareX, this handles file operations that need consensus.
 */
@Component
public class RaftStateMachine {
    private static final Logger logger = LoggerFactory.getLogger(RaftStateMachine.class);
    
    // Track applied operations for idempotency
    private final ConcurrentHashMap<String, Long> appliedOperations = new ConcurrentHashMap<>();
    
    // Track consensus-approved files and their properties
    private final ConcurrentHashMap<String, String> approvedFiles = new ConcurrentHashMap<>();
    
    /**
     * Apply a committed log entry to the state machine.
     */
    public void apply(RaftLogEntry entry) {
        try {
            String command = entry.getCommand();
            String data = entry.getData();
            
            logger.info("Applying log entry {}: {} with data: {}", entry.getIndex(), command, data);
            
            // Parse command and execute
            if (command.startsWith("UPLOAD:")) {
                handleUpload(command.substring(7), data);
            } else if (command.startsWith("DELETE:")) {
                handleDelete(command.substring(7), data);
            } else if (command.startsWith("SYNC:")) {
                handleSync(command.substring(5), data);
            } else if (command.equals("HEARTBEAT")) {
                // Heartbeat - no action needed
                logger.debug("Processed heartbeat at index {}", entry.getIndex());
            } else {
                logger.warn("Unknown command: {}", command);
            }
            
            // Mark as applied
            appliedOperations.put(entry.getIndex() + ":" + command, entry.getTimestamp());
            
        } catch (Exception e) {
            logger.error("Error applying log entry {}: {}", entry.getIndex(), entry, e);
            throw new RuntimeException("Failed to apply log entry", e);
        }
    }
    
    /**
     * Handle file upload operation.
     */
    private void handleUpload(String filename, String metadata) {
        logger.info("Processing consensus for file upload: {}", filename);
        
        // Update the consensus-approved file registry
        approvedFiles.put(filename, metadata);
        
        logger.info("File upload consensus completed: {} (metadata: {})", filename, metadata);
    }
    
    /**
     * Handle file delete operation.
     */
    private void handleDelete(String filename, String reason) {
        logger.info("Processing consensus for file delete: {}", filename);
        
        // Remove from the consensus-approved file registry
        approvedFiles.remove(filename);
        
        logger.info("File delete consensus completed: {} (reason: {})", filename, reason);
    }
    
    /**
     * Handle file synchronization operation.
     */
    private void handleSync(String serverId, String fileList) {
        logger.info("Processing sync operation for server: {}", serverId);
        
        // In a real implementation, this would:
        // 1. Parse file list from syncing server
        // 2. Identify missing files
        // 3. Trigger file transfers
        
        logger.info("Sync operation completed for server: {}", serverId);
    }
    
    /**
     * Check if an operation has been applied.
     */
    public boolean isApplied(int index, String command) {
        return appliedOperations.containsKey(index + ":" + command);
    }
    
    /**
     * Get the current state of the state machine.
     */
    public ConcurrentHashMap<String, Long> getAppliedOperations() {
        return new ConcurrentHashMap<>(appliedOperations);
    }
    
    /**
     * Reset the state machine (for testing).
     */
    public void reset() {
        appliedOperations.clear();
        approvedFiles.clear();
        logger.info("State machine reset");
    }
}
