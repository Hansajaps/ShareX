package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance metrics for Raft consensus algorithm.
 * Tracks timing, throughput, and failure rates.
 */
@Component
public class RaftMetrics {
    private static final Logger logger = LoggerFactory.getLogger(RaftMetrics.class);
    
    // Timing metrics
    private final AtomicLong totalElectionTime = new AtomicLong(0);
    private final AtomicLong totalConsensusTime = new AtomicLong(0);
    private final AtomicLong electionStartTime = new AtomicLong(0);
    private final AtomicLong consensusStartTime = new AtomicLong(0);
    
    // Count metrics
    private final AtomicInteger leaderElections = new AtomicInteger(0);
    private final AtomicInteger commandsSubmitted = new AtomicInteger(0);
    private final AtomicInteger commandsCommitted = new AtomicInteger(0);
    private final AtomicInteger heartbeatsSent = new AtomicInteger(0);
    private final AtomicInteger heartbeatsReceived = new AtomicInteger(0);
    private final AtomicInteger rpcSent = new AtomicInteger(0);
    private final AtomicInteger rpcReceived = new AtomicInteger(0);
    private final AtomicInteger votesRequested = new AtomicInteger(0);
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final AtomicInteger failedRPCs = new AtomicInteger(0);
    
    // Performance metrics
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile boolean isLeader = false;
    
    /**
     * Start timing an election.
     */
    public void startElectionTiming() {
        electionStartTime.set(System.currentTimeMillis());
    }
    
    /**
     * End timing an election and record the duration.
     */
    public void endElectionTiming() {
        long startTime = electionStartTime.get();
        if (startTime > 0) {
            long duration = System.currentTimeMillis() - startTime;
            totalElectionTime.addAndGet(duration);
            leaderElections.incrementAndGet();
            logger.info("Election completed in {}ms", duration);
            electionStartTime.set(0);
        }
    }
    
    /**
     * Start timing consensus for a command.
     */
    public void startConsensusTiming() {
        consensusStartTime.set(System.currentTimeMillis());
    }
    
    /**
     * End timing consensus and record the duration.
     */
    public void endConsensusTiming() {
        long startTime = consensusStartTime.get();
        if (startTime > 0) {
            long duration = System.currentTimeMillis() - startTime;
            totalConsensusTime.addAndGet(duration);
            commandsCommitted.incrementAndGet();
            logger.debug("Consensus reached in {}ms", duration);
            consensusStartTime.set(0);
        }
    }
    
    // Count increment methods
    
    public void incrementLeaderElections() {
        leaderElections.incrementAndGet();
    }
    
    public void incrementCommandsSubmitted() {
        commandsSubmitted.incrementAndGet();
    }
    
    public void incrementHeartbeatsSent() {
        heartbeatsSent.incrementAndGet();
        rpcSent.incrementAndGet();
    }
    
    public void incrementHeartbeatsReceived() {
        heartbeatsReceived.incrementAndGet();
        rpcReceived.incrementAndGet();
    }
    
    public void incrementRPCSent() {
        rpcSent.incrementAndGet();
    }
    
    public void incrementRPCReceived() {
        rpcReceived.incrementAndGet();
    }
    
    public void incrementVotesRequested() {
        votesRequested.incrementAndGet();
        rpcSent.incrementAndGet();
    }
    
    public void incrementVotesReceived() {
        votesReceived.incrementAndGet();
        rpcReceived.incrementAndGet();
    }
    
    public void incrementFailedRPCs() {
        failedRPCs.incrementAndGet();
    }
    
    // State update methods
    
    public void setCurrentTerm(int term) {
        currentTerm.set(term);
    }
    
    public void setLeader(boolean leader) {
        isLeader = leader;
    }
    
    // Performance calculation methods
    
    public double getAverageElectionTime() {
        int elections = leaderElections.get();
        return elections > 0 ? (double) totalElectionTime.get() / elections : 0;
    }
    
    public double getAverageConsensusTime() {
        int committed = commandsCommitted.get();
        return committed > 0 ? (double) totalConsensusTime.get() / committed : 0;
    }
    
    public double getCommandsThroughput() {
        long uptime = System.currentTimeMillis() - startTime.get();
        return uptime > 0 ? (double) commandsCommitted.get() / (uptime / 1000.0) : 0;
    }
    
    public double getRPCFailureRate() {
        int sent = rpcSent.get();
        return sent > 0 ? (double) failedRPCs.get() / sent : 0;
    }
    
    public double getHeartbeatRate() {
        long uptime = System.currentTimeMillis() - startTime.get();
        return uptime > 0 ? (double) heartbeatsSent.get() / (uptime / 1000.0) : 0;
    }
    
    // Getters for all metrics
    
    public int getLeaderElections() {
        return leaderElections.get();
    }
    
    public int getCommandsSubmitted() {
        return commandsSubmitted.get();
    }
    
    public int getCommandsCommitted() {
        return commandsCommitted.get();
    }
    
    public int getHeartbeatsSent() {
        return heartbeatsSent.get();
    }
    
    public int getHeartbeatsReceived() {
        return heartbeatsReceived.get();
    }
    
    public int getRPCSent() {
        return rpcSent.get();
    }
    
    public int getRPCReceived() {
        return rpcReceived.get();
    }
    
    public int getVotesRequested() {
        return votesRequested.get();
    }
    
    public int getVotesReceived() {
        return votesReceived.get();
    }
    
    public int getFailedRPCs() {
        return failedRPCs.get();
    }
    
    public int getCurrentTerm() {
        return currentTerm.get();
    }
    
    public boolean isLeader() {
        return isLeader;
    }
    
    public long getUptime() {
        return System.currentTimeMillis() - startTime.get();
    }
    
    /**
     * Get a comprehensive metrics report.
     */
    public String getMetricsReport() {
        StringBuilder report = new StringBuilder();
        report.append("\n=== Raft Consensus Metrics ===\n");
        report.append(String.format("Uptime: %.1f seconds\n", getUptime() / 1000.0));
        report.append(String.format("Current Term: %d\n", getCurrentTerm()));
        report.append(String.format("Is Leader: %s\n", isLeader ? "Yes" : "No"));
        
        report.append("\n--- Election Metrics ---\n");
        report.append(String.format("Leader Elections: %d\n", getLeaderElections()));
        report.append(String.format("Average Election Time: %.2f ms\n", getAverageElectionTime()));
        
        report.append("\n--- Command Metrics ---\n");
        report.append(String.format("Commands Submitted: %d\n", getCommandsSubmitted()));
        report.append(String.format("Commands Committed: %d\n", getCommandsCommitted()));
        report.append(String.format("Average Consensus Time: %.2f ms\n", getAverageConsensusTime()));
        report.append(String.format("Commands Throughput: %.2f cmd/sec\n", getCommandsThroughput()));
        
        report.append("\n--- Communication Metrics ---\n");
        report.append(String.format("RPCs Sent: %d\n", getRPCSent()));
        report.append(String.format("RPCs Received: %d\n", getRPCReceived()));
        report.append(String.format("Failed RPCs: %d\n", getFailedRPCs()));
        report.append(String.format("RPC Failure Rate: %.2f%%\n", getRPCFailureRate() * 100));
        report.append(String.format("Heartbeats Sent: %d\n", getHeartbeatsSent()));
        report.append(String.format("Heartbeats Received: %d\n", getHeartbeatsReceived()));
        report.append(String.format("Heartbeat Rate: %.2f Hz\n", getHeartbeatRate()));
        
        report.append("\n--- Voting Metrics ---\n");
        report.append(String.format("Votes Requested: %d\n", getVotesRequested()));
        report.append(String.format("Votes Received: %d\n", getVotesReceived()));
        
        return report.toString();
    }
    
    /**
     * Reset all metrics (for testing).
     */
    public void reset() {
        totalElectionTime.set(0);
        totalConsensusTime.set(0);
        electionStartTime.set(0);
        consensusStartTime.set(0);
        leaderElections.set(0);
        commandsSubmitted.set(0);
        commandsCommitted.set(0);
        heartbeatsSent.set(0);
        heartbeatsReceived.set(0);
        rpcSent.set(0);
        rpcReceived.set(0);
        votesRequested.set(0);
        votesReceived.set(0);
        failedRPCs.set(0);
        startTime.set(System.currentTimeMillis());
        currentTerm.set(0);
        isLeader = false;
        
        logger.info("Raft metrics reset");
    }
}
