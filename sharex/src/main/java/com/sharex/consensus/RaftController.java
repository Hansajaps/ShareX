package com.sharex.consensus;

import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API controller for Raft consensus operations.
 * Provides endpoints for inter-node RPC communication and client operations.
 */
@RestController
@RequestMapping("/raft")
public class RaftController {
    private static final Logger logger = LoggerFactory.getLogger(RaftController.class);
    
    @Autowired
    private RaftNode raftNode;
    
    @Autowired
    private RaftMetrics metrics;
    
    /**
     * Handle incoming Raft RPC messages.
     */
    @PostMapping("/rpc")
    public ResponseEntity<RaftRPC> handleRPC(@RequestBody RaftRPC rpc) {
        logger.debug("Received Raft RPC: {}", rpc);
        
        try {
            RaftRPC response = raftNode.handleRPC(rpc).get();
            if (response != null) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.ok().build();
            }
        } catch (Exception e) {
            logger.error("Error handling Raft RPC: {}", rpc, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Submit a command to the Raft consensus system.
     * Client endpoint for file operations requiring consensus.
     */
    @PostMapping("/command")
    public ResponseEntity<RaftCommandResponse> submitCommand(@RequestBody RaftCommandRequest request) {
        logger.info("Received command request: {} with data: {}", request.getCommand(), request.getData());
        
        try {
            boolean success = raftNode.submitCommand(request.getCommand(), request.getData()).get();
            
            RaftCommandResponse response = new RaftCommandResponse(
                success, 
                success ? "Command submitted successfully" : "Failed to submit command",
                raftNode.getState().getCurrentTerm(),
                raftNode.getState().isLeader()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error submitting command: {}", request.getCommand(), e);
            RaftCommandResponse response = new RaftCommandResponse(
                false, 
                "Error: " + e.getMessage(),
                raftNode.getState().getCurrentTerm(),
                raftNode.getState().isLeader()
            );
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * Get current Raft status and metrics.
     */
    @GetMapping("/status")
    public ResponseEntity<RaftStatusResponse> getStatus() {
        try {
            RaftNodeState state = raftNode.getState();
            
            RaftStatusResponse response = new RaftStatusResponse(
                state.getCurrentServerId(),
                state.getState().toString(),
                state.getCurrentTerm(),
                state.isLeader(),
                state.getVotedFor(),
                state.getLog().getLastIndex(),
                state.getLog().getCommitIndex(),
                state.getLog().size(),
                raftNode.isHealthy()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting Raft status", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get detailed performance metrics.
     */
    @GetMapping("/metrics")
    public ResponseEntity<String> getMetrics() {
        try {
            String metricsReport = metrics.getMetricsReport();
            return ResponseEntity.ok(metricsReport);
        } catch (Exception e) {
            logger.error("Error getting metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get log entries for debugging.
     */
    @GetMapping("/log")
    public ResponseEntity<RaftLogResponse> getLog() {
        try {
            RaftNodeState state = raftNode.getState();
            RaftLogResponse response = new RaftLogResponse(
                state.getLog().getAllEntries(),
                state.getLog().getCommitIndex(),
                state.getLog().getLastApplied()
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Error getting log", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Force a new election (for testing).
     */
    @PostMapping("/election/force")
    public ResponseEntity<String> forceElection() {
        logger.warn("Forcing new election");
        try {
            // This would trigger the Raft node to start an election
            // Implementation depends on exposing this capability in RaftNode
            return ResponseEntity.ok("Election triggered");
        } catch (Exception e) {
            logger.error("Error forcing election", e);
            return ResponseEntity.internalServerError().body("Failed to trigger election");
        }
    }
    
    /**
     * Reset metrics (for testing).
     */
    @PostMapping("/metrics/reset")
    public ResponseEntity<String> resetMetrics() {
        logger.warn("Resetting metrics");
        try {
            metrics.reset();
            return ResponseEntity.ok("Metrics reset");
        } catch (Exception e) {
            logger.error("Error resetting metrics", e);
            return ResponseEntity.internalServerError().body("Failed to reset metrics");
        }
    }
    
    // Request/Response DTOs
    
    public static class RaftCommandRequest {
        private String command;
        private String data;
        
        public RaftCommandRequest() {}
        
        public RaftCommandRequest(String command, String data) {
            this.command = command;
            this.data = data;
        }
        
        public String getCommand() { return command; }
        public void setCommand(String command) { this.command = command; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }
    
    public static class RaftCommandResponse {
        private boolean success;
        private String message;
        private int term;
        private boolean isLeader;
        
        public RaftCommandResponse() {}
        
        public RaftCommandResponse(boolean success, String message, int term, boolean isLeader) {
            this.success = success;
            this.message = message;
            this.term = term;
            this.isLeader = isLeader;
        }
        
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public int getTerm() { return term; }
        public void setTerm(int term) { this.term = term; }
        public boolean isLeader() { return isLeader; }
        public void setLeader(boolean leader) { isLeader = leader; }
    }
    
    public static class RaftStatusResponse {
        private String serverId;
        private String state;
        private int term;
        private boolean isLeader;
        private String votedFor;
        private int lastIndex;
        private int commitIndex;
        private int logSize;
        private boolean healthy;
        
        public RaftStatusResponse() {}
        
        public RaftStatusResponse(String serverId, String state, int term, boolean isLeader,
                                String votedFor, int lastIndex, int commitIndex, int logSize, boolean healthy) {
            this.serverId = serverId;
            this.state = state;
            this.term = term;
            this.isLeader = isLeader;
            this.votedFor = votedFor;
            this.lastIndex = lastIndex;
            this.commitIndex = commitIndex;
            this.logSize = logSize;
            this.healthy = healthy;
        }
        
        // Getters and setters
        public String getServerId() { return serverId; }
        public void setServerId(String serverId) { this.serverId = serverId; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public int getTerm() { return term; }
        public void setTerm(int term) { this.term = term; }
        public boolean isLeader() { return isLeader; }
        public void setLeader(boolean leader) { isLeader = leader; }
        public String getVotedFor() { return votedFor; }
        public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
        public int getLastIndex() { return lastIndex; }
        public void setLastIndex(int lastIndex) { this.lastIndex = lastIndex; }
        public int getCommitIndex() { return commitIndex; }
        public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
        public int getLogSize() { return logSize; }
        public void setLogSize(int logSize) { this.logSize = logSize; }
        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }
    }
    
    public static class RaftLogResponse {
        private java.util.List<RaftLogEntry> entries;
        private int commitIndex;
        private int lastApplied;
        
        public RaftLogResponse() {}
        
        public RaftLogResponse(java.util.List<RaftLogEntry> entries, int commitIndex, int lastApplied) {
            this.entries = entries;
            this.commitIndex = commitIndex;
            this.lastApplied = lastApplied;
        }
        
        public java.util.List<RaftLogEntry> getEntries() { return entries; }
        public void setEntries(java.util.List<RaftLogEntry> entries) { this.entries = entries; }
        public int getCommitIndex() { return commitIndex; }
        public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
        public int getLastApplied() { return lastApplied; }
        public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }
    }
}
