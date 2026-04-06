package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fault detection and recovery mechanisms for Raft consensus.
 * Handles network partitions, node failures, and recovery scenarios.
 */
@Component
public class RaftFaultHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftFaultHandler.class);
    
    private final RaftNode raftNode;
    private final RaftMetrics metrics;
    
    // Fault detection state
    private final ConcurrentHashMap<String, NodeHealth> nodeHealth = new ConcurrentHashMap<>();
    private final AtomicBoolean networkPartitionDetected = new AtomicBoolean(false);
    private final AtomicLong lastHealthCheck = new AtomicLong(System.currentTimeMillis());
    
    // Configuration
    private static final long HEALTH_CHECK_INTERVAL = 5000; // 5 seconds
    private static final long NODE_TIMEOUT = 15000; // 15 seconds
    private static final int MAX_FAILURES = 3;
    
    public RaftFaultHandler(RaftNode raftNode, RaftMetrics metrics) {
        this.raftNode = raftNode;
        this.metrics = metrics;
        
        // Initialize node health tracking
        String[] servers = {"server1", "server2", "server3"};
        for (String serverId : servers) {
            nodeHealth.put(serverId, new NodeHealth(serverId));
        }
        
        logger.info("RaftFaultHandler initialized for {} nodes", servers.length);
    }
    
    /**
     * Periodic health check for all nodes.
     */
    @Scheduled(fixedDelay = HEALTH_CHECK_INTERVAL)
    public void performHealthCheck() {
        try {
            long now = System.currentTimeMillis();
            lastHealthCheck.set(now);
            
            logger.debug("Performing health check on {} nodes", nodeHealth.size());
            
            // Check health of all nodes
            for (NodeHealth health : nodeHealth.values()) {
                checkNodeHealth(health, now);
            }
            
            // Detect network partitions
            detectNetworkPartitions();
            
            // Trigger recovery if needed
            triggerRecoveryIfNeeded();
            
        } catch (Exception e) {
            logger.error("Error during health check", e);
        }
    }
    
    /**
     * Check health of a specific node.
     */
    private void checkNodeHealth(NodeHealth health, long now) {
        String nodeId = health.getNodeId();
        
        // Skip self-check
        if (nodeId.equals(raftNode.getState().getCurrentServerId())) {
            health.markHealthy(now);
            return;
        }
        
        // Check if node has responded recently
        long timeSinceLastResponse = now - health.getLastResponseTime();
        
        if (timeSinceLastResponse > NODE_TIMEOUT) {
            // Node appears to be down
            health.markUnhealthy(now);
            logger.warn("Node {} appears to be down (no response for {}ms)", nodeId, timeSinceLastResponse);
            
            // Increment failure count
            metrics.incrementFailedRPCs();
            
            // If this node is the leader and we're a follower, we might need to start election
            if (health.isPotentiallyLeader() && raftNode.getState().isFollower()) {
                logger.info("Potential leader failure detected - may trigger election");
                // The Raft election timeout will handle this naturally
            }
        } else {
            health.markHealthy(now);
        }
    }
    
    /**
     * Detect network partitions.
     */
    private void detectNetworkPartitions() {
        int healthyNodes = 0;
        int totalNodes = nodeHealth.size();
        
        for (NodeHealth health : nodeHealth.values()) {
            if (health.isHealthy()) {
                healthyNodes++;
            }
        }
        
        // Check if we have a majority of healthy nodes
        boolean hasMajority = healthyNodes > totalNodes / 2;
        boolean wasPartitioned = networkPartitionDetected.get();
        
        if (!hasMajority && !wasPartitioned) {
            // Network partition detected
            networkPartitionDetected.set(true);
            logger.warn("Network partition detected! Only {}/{} nodes are healthy", healthyNodes, totalNodes);
            
            // If we're not the leader, step down
            if (raftNode.getState().isLeader()) {
                logger.info("Stepping down as leader due to network partition");
                raftNode.getState().becomeFollower();
            }
            
        } else if (hasMajority && wasPartitioned) {
            // Network partition healed
            networkPartitionDetected.set(false);
            logger.info("Network partition healed! {}/{} nodes are healthy", healthyNodes, totalNodes);
            
            // If we were a follower, we can now participate normally
            if (raftNode.getState().isFollower()) {
                logger.info("Network partition healed - resuming normal operation");
            }
        }
    }
    
    /**
     * Trigger recovery mechanisms if needed.
     */
    private void triggerRecoveryIfNeeded() {
        // Check if we need to recover from a partition
        if (networkPartitionDetected.get()) {
            return; // Don't attempt recovery during partition
        }
        
        // Check for nodes that have recovered
        for (NodeHealth health : nodeHealth.values()) {
            if (health.hasRecovered()) {
                logger.info("Node {} has recovered - initiating sync", health.getNodeId());
                initiateNodeRecovery(health);
                health.clearRecovered();
            }
        }
    }
    
    /**
     * Initiate recovery for a node that came back online.
     */
    private void initiateNodeRecovery(NodeHealth health) {
        String nodeId = health.getNodeId();
        
        try {
            // If we're the leader, we need to sync the recovered node
            if (raftNode.getState().isLeader()) {
                logger.info("Leader initiating recovery sync for node {}", nodeId);
                
                // Send a sync command with current log state
                RaftNodeState state = raftNode.getState();
                String syncData = String.format("lastIndex=%d,commitIndex=%d", 
                                              state.getLog().getLastIndex(), 
                                              state.getCommitIndex());
                
                raftNode.submitCommand("SYNC:" + nodeId, syncData);
                
            } else {
                // If we're not the leader, just log the recovery
                logger.info("Node {} recovered (not leader - sync will be handled by leader)", nodeId);
            }
            
        } catch (Exception e) {
            logger.error("Error initiating recovery for node {}", nodeId, e);
        }
    }
    
    /**
     * Handle node failure notification.
     */
    public void handleNodeFailure(String nodeId, String reason) {
        logger.warn("Node failure reported: {} - {}", nodeId, reason);
        
        NodeHealth health = nodeHealth.get(nodeId);
        if (health != null) {
            health.markFailed(reason);
            metrics.incrementFailedRPCs();
        }
        
        // If the failed node was the leader and we're a follower, start election
        if (health != null && health.isPotentiallyLeader() && raftNode.getState().isFollower()) {
            logger.info("Leader node {} failed - election may be triggered", nodeId);
        }
    }
    
    /**
     * Handle network partition simulation.
     */
    public void simulateNetworkPartition(boolean isolate) {
        if (isolate) {
            logger.warn("SIMULATION: Simulating network partition");
            networkPartitionDetected.set(true);
            
            // Mark all other nodes as unhealthy
            for (NodeHealth health : nodeHealth.values()) {
                if (!health.getNodeId().equals(raftNode.getState().getCurrentServerId())) {
                    health.markUnhealthy(System.currentTimeMillis());
                }
            }
            
            // Step down if we're leader
            if (raftNode.getState().isLeader()) {
                raftNode.getState().becomeFollower();
            }
            
        } else {
            logger.info("SIMULATION: Healing network partition");
            networkPartitionDetected.set(false);
            
            // Mark all nodes as healthy
            long now = System.currentTimeMillis();
            for (NodeHealth health : nodeHealth.values()) {
                health.markHealthy(now);
            }
        }
    }
    
    /**
     * Get fault handler status.
     */
    public FaultHandlerStatus getStatus() {
        int healthyNodes = 0;
        int unhealthyNodes = 0;
        
        for (NodeHealth health : nodeHealth.values()) {
            if (health.isHealthy()) {
                healthyNodes++;
            } else {
                unhealthyNodes++;
            }
        }
        
        return new FaultHandlerStatus(
            healthyNodes,
            unhealthyNodes,
            networkPartitionDetected.get(),
            lastHealthCheck.get()
        );
    }
    
    /**
     * Reset fault handler state (for testing).
     */
    public void reset() {
        networkPartitionDetected.set(false);
        lastHealthCheck.set(System.currentTimeMillis());
        
        for (NodeHealth health : nodeHealth.values()) {
            health.reset();
        }
        
        logger.info("RaftFaultHandler reset");
    }
    
    // Inner classes
    
    private static class NodeHealth {
        private final String nodeId;
        private volatile boolean healthy = true;
        private volatile long lastResponseTime = System.currentTimeMillis();
        private volatile int failureCount = 0;
        private volatile boolean recovered = false;
        private volatile String lastFailureReason = "";
        
        public NodeHealth(String nodeId) {
            this.nodeId = nodeId;
        }
        
        public String getNodeId() { return nodeId; }
        public boolean isHealthy() { return healthy; }
        public long getLastResponseTime() { return lastResponseTime; }
        public int getFailureCount() { return failureCount; }
        public boolean hasRecovered() { return recovered; }
        public String getLastFailureReason() { return lastFailureReason; }
        
        public void markHealthy(long now) {
            if (!healthy) {
                recovered = true;
                logger.info("Node {} marked as healthy again", nodeId);
            }
            healthy = true;
            lastResponseTime = now;
            failureCount = 0;
        }
        
        public void markUnhealthy(long now) {
            healthy = false;
            failureCount++;
            lastResponseTime = now;
        }
        
        public void markFailed(String reason) {
            healthy = false;
            failureCount++;
            lastFailureReason = reason;
            lastResponseTime = System.currentTimeMillis();
            logger.warn("Node {} marked as failed: {} (failure #{})", nodeId, reason, failureCount);
        }
        
        public void clearRecovered() {
            recovered = false;
        }
        
        public boolean isPotentiallyLeader() {
            // Simple heuristic: any node could be the leader
            // In practice, this would be based on actual leader knowledge
            return true;
        }
        
        public void reset() {
            healthy = true;
            lastResponseTime = System.currentTimeMillis();
            failureCount = 0;
            recovered = false;
            lastFailureReason = "";
        }
    }
    
    public static class FaultHandlerStatus {
        private final int healthyNodes;
        private final int unhealthyNodes;
        private final boolean networkPartitionDetected;
        private final long lastHealthCheck;
        
        public FaultHandlerStatus(int healthyNodes, int unhealthyNodes, 
                                boolean networkPartitionDetected, long lastHealthCheck) {
            this.healthyNodes = healthyNodes;
            this.unhealthyNodes = unhealthyNodes;
            this.networkPartitionDetected = networkPartitionDetected;
            this.lastHealthCheck = lastHealthCheck;
        }
        
        public int getHealthyNodes() { return healthyNodes; }
        public int getUnhealthyNodes() { return unhealthyNodes; }
        public boolean isNetworkPartitionDetected() { return networkPartitionDetected; }
        public long getLastHealthCheck() { return lastHealthCheck; }
        
        @Override
        public String toString() {
            return String.format("FaultHandlerStatus{healthy=%d, unhealthy=%d, partition=%s, lastCheck=%d}", 
                               healthyNodes, unhealthyNodes, networkPartitionDetected, lastHealthCheck);
        }
    }
}
