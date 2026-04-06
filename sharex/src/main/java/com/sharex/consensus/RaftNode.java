package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core Raft consensus implementation.
 * Handles leader election, log replication, and state machine commands.
 */
@Component
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
    
    private final RaftNodeState state;
    private final RaftRPCService rpcService;
    private final RaftStateMachine stateMachine;
    private final RaftMetrics metrics;
    
    // Cluster configuration
    private final ConcurrentHashMap<String, Integer> serverIndexMap = new ConcurrentHashMap<>();
    private final String[] serverIds;
    private final int clusterSize;
    
    // Election state
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final AtomicLong lastHeartbeatSent = new AtomicLong(0);
    
    private String currentServerId;
    
    public RaftNode(RaftNodeState state, RaftRPCService rpcService, 
                   RaftStateMachine stateMachine, RaftMetrics metrics) {
        this.state = state;
        this.rpcService = rpcService;
        this.stateMachine = stateMachine;
        this.metrics = metrics;
        
        // Initialize cluster configuration
        this.serverIds = new String[]{"server1", "server2", "server3"};
        this.clusterSize = serverIds.length;
        
        for (int i = 0; i < serverIds.length; i++) {
            serverIndexMap.put(serverIds[i], i);
        }
        
        logger.info("Initialized RaftNode for cluster of {} nodes", clusterSize);
    }
    
    public void setCurrentServerId(String serverId) {
        this.currentServerId = serverId;
        this.state.setCurrentServerId(serverId);
    }
    
    /**
     * Main Raft loop - runs periodically to handle timeouts and heartbeats.
     */
    @Scheduled(fixedDelay = 100) // Run every 100ms
    public void raftLoop() {
        try {
            if (state.isLeader()) {
                sendHeartbeats();
                checkCommitIndex();
            } else if (state.isCandidate()) {
                checkElectionTimeout();
            } else if (state.isFollower()) {
                checkElectionTimeout();
            }
            
            // Apply committed entries to state machine
            applyCommittedEntries();
            
        } catch (Exception e) {
            logger.error("Error in Raft loop", e);
        }
    }
    
    /**
     * Handle incoming RPC messages.
     */
    public CompletableFuture<RaftRPC> handleRPC(RaftRPC rpc) {
        metrics.incrementRPCReceived();
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                switch (rpc.getType()) {
                    case REQUEST_VOTE:
                        return handleRequestVote(rpc);
                    case APPEND_ENTRIES:
                        return handleAppendEntries(rpc);
                    case VOTE_RESPONSE:
                        return handleVoteResponse(rpc);
                    case APPEND_RESPONSE:
                        return handleAppendResponse(rpc);
                    default:
                        logger.warn("Unknown RPC type: {}", rpc.getType());
                        return null;
                }
            } catch (Exception e) {
                logger.error("Error handling RPC: {}", rpc, e);
                return null;
            }
        });
    }
    
    /**
     * Submit a client command to the Raft log.
     */
    public CompletableFuture<Boolean> submitCommand(String command, String data) {
        if (!state.isLeader()) {
            logger.warn("Rejecting command - not leader. Current state: {}", state.getState());
            return CompletableFuture.completedFuture(false);
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Append entry to log
                int index = state.getLog().appendEntry(state.getCurrentTerm(), command, data);
                logger.info("Appended command {} at index {}", command, index);
                
                // Send to followers (this will be handled by the periodic heartbeat)
                metrics.incrementCommandsSubmitted();
                return true;
                
            } catch (Exception e) {
                logger.error("Error submitting command: {}", command, e);
                return false;
            }
        });
    }
    
    // RPC Handlers
    
    private void checkTerm(int incomingTerm) {
        if (incomingTerm > state.getCurrentTerm()) {
            logger.info("Saw higher term {}. Current term: {}. Stepping down.", incomingTerm, state.getCurrentTerm());
            state.setCurrentTerm(incomingTerm);
            state.becomeFollower();
        }
    }
    
    private RaftRPC handleRequestVote(RaftRPC rpc) {
        logger.debug("Received RequestVote: {}", rpc);
        
        // Step down if higher term seen
        checkTerm(rpc.getTerm());
        
        // Check if we can grant vote
        boolean voteGranted = false;
        if (rpc.getTerm() == state.getCurrentTerm()) {
            String votedFor = state.getVotedFor();
            if (votedFor == null || votedFor.equals(rpc.getCandidateId())) {
                // Check if candidate's log is at least as up-to-date as ours
                RaftLogEntry lastEntry = state.getLog().getLastEntry();
                if (rpc.getLastLogTerm() > lastEntry.getTerm() ||
                    (rpc.getLastLogTerm() == lastEntry.getTerm() && rpc.getLastLogIndex() >= lastEntry.getIndex())) {
                    
                    voteGranted = true;
                    state.setVotedFor(rpc.getCandidateId());
                    state.resetElectionTimeout();
                }
            }
        }
        
        logger.info("Vote {} for candidate {} in term {}", 
                   voteGranted ? "granted" : "denied", rpc.getCandidateId(), rpc.getTerm());
        
        return new RaftRPC(currentServerId, state.getCurrentTerm(), voteGranted, "VOTE_RESPONSE");
    }
    
    private RaftRPC handleAppendEntries(RaftRPC rpc) {
        logger.debug("Received AppendEntries: {}", rpc);
        
        // Step down if higher term seen
        checkTerm(rpc.getTerm());
        
        boolean success = false;
        
        if (rpc.getTerm() == state.getCurrentTerm()) {
            // If we are candidate or leader and see same term heartbeat, we are no longer leader/candidate
            if (!state.isFollower()) {
                state.becomeFollower();
            }
            // Update leader info
            state.setLeaderId(rpc.getSenderId());
            // Accept heartbeat and update election timeout
            state.updateLastHeartbeat();
            state.resetElectionTimeout();
            
            // Check if log entry at prevLogIndex matches prevLogTerm
            if (rpc.getLastLogIndex() == 0 || state.getLog().matchEntry(rpc.getLastLogIndex(), rpc.getLastLogTerm())) {
                success = true;
                
                // Append new entries
                if (rpc.getEntries() != null && rpc.getEntries().length > 0) {
                    for (RaftLogEntry entry : rpc.getEntries()) {
                        // Overwrite any conflicting entries
                        if (state.getLog().getEntry(entry.getIndex()) != null) {
                            state.getLog().truncateFrom(entry.getIndex());
                        }
                        // Re-append the entry
                        state.getLog().appendEntry(entry.getTerm(), entry.getCommand(), entry.getData());
                    }
                    logger.info("Appended {} entries from leader", rpc.getEntries().length);
                }
                
                // Update commit index
                if (rpc.getLeaderCommit() > state.getCommitIndex()) {
                    int newCommitIndex = Math.min(rpc.getLeaderCommit(), state.getLog().getLastIndex());
                    state.setCommitIndex(newCommitIndex);
                    logger.debug("Updated commit index to {}", newCommitIndex);
                }
            } else {
                logger.debug("Log inconsistency at index {}/{}", rpc.getLastLogIndex(), rpc.getLastLogTerm());
            }
        }
        
        return new RaftRPC(currentServerId, state.getCurrentTerm(), success, 0);
    }
    
    private RaftRPC handleVoteResponse(RaftRPC rpc) {
        logger.debug("Received VoteResponse: {}", rpc);
        
        checkTerm(rpc.getTerm());
        
        if (state.isCandidate() && rpc.getTerm() == state.getCurrentTerm()) {
            if (rpc.isVoteGranted()) {
                int votes = votesReceived.incrementAndGet();
                logger.info("Received vote from {}. Total votes: {}", rpc.getSenderId(), votes);
                
                // Check if we have majority
                if (votes > clusterSize / 2) {
                    state.becomeLeader();
                    metrics.incrementLeaderElections();
                    logger.info("Won election with {} votes! Became leader for term {}.", votes, state.getCurrentTerm());
                }
            }
        }
        
        return null; // No response needed for vote responses
    }
    
    private RaftRPC handleAppendResponse(RaftRPC rpc) {
        logger.debug("Received AppendResponse: {}", rpc);
        
        checkTerm(rpc.getTerm());
        
        if (state.isLeader() && rpc.getTerm() == state.getCurrentTerm()) {
            Integer serverIndexObj = serverIndexMap.get(rpc.getSenderId());
            if (serverIndexObj != null) {
                int serverIndex = serverIndexObj;
                if (rpc.isSuccess()) {
                    // Success - update nextIndex and matchIndex
                    RaftLogEntry[] entries = state.getLog().getEntriesFrom(state.getMatchIndex(serverIndex) + 1);
                    if (entries.length > 0) {
                        state.setMatchIndex(serverIndex, entries[entries.length - 1].getIndex());
                        state.setNextIndex(serverIndex, entries[entries.length - 1].getIndex() + 1);
                    }
                } else {
                    // Failure - decrement nextIndex and retry
                    state.setNextIndex(serverIndex, Math.max(1, state.getNextIndex(serverIndex) - 1));
                }
            }
        } else if (rpc.getTerm() > state.getCurrentTerm()) {
            state.setCurrentTerm(rpc.getTerm());
            state.becomeFollower();
        }
        
        return null; // No response needed for append responses
    }
    
    // Periodic tasks
    
    private void sendHeartbeats() {
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatSent.get() < 100) { // Send heartbeats every 100ms
            return;
        }
        
        lastHeartbeatSent.set(now);
        
        for (String serverId : serverIds) {
            if (!serverId.equals(currentServerId)) {
                sendAppendEntries(serverId);
            }
        }
    }
    
    private void sendAppendEntries(String followerId) {
        try {
            int serverIndex = serverIndexMap.get(followerId);
            int nextIndex = state.getNextIndex(serverIndex);
            
            // Get entries to send
            RaftLogEntry prevEntry = state.getLog().getEntry(nextIndex - 1);
            int prevLogIndex = prevEntry != null ? prevEntry.getIndex() : 0;
            int prevLogTerm = prevEntry != null ? prevEntry.getTerm() : 0;
            
            RaftLogEntry[] entries = state.getLog().getEntriesFrom(nextIndex);
            
            RaftRPC rpc = new RaftRPC(currentServerId, state.getCurrentTerm(), 
                                    prevLogIndex, prevLogTerm, entries, state.getCommitIndex());
            
            rpcService.sendRPC(followerId, rpc).thenAccept(response -> {
                if (response != null) {
                    // Responses are processed as incoming RPCs
                    handleRPC(response);
                }
            });
            metrics.incrementHeartbeatsSent();
            
        } catch (Exception e) {
            logger.error("Error sending AppendEntries to {}", followerId, e);
        }
    }
    
    private void checkElectionTimeout() {
        if (state.isElectionTimeoutExpired()) {
            logger.info("Election timeout expired - starting new election");
            startElection();
        }
    }
    
    private void startElection() {
        state.becomeCandidate();
        votesReceived.set(1); // Vote for self
        
        // Send RequestVote to all other servers
        RaftLogEntry lastEntry = state.getLog().getLastEntry();
        
        for (String serverId : serverIds) {
            if (!serverId.equals(currentServerId)) {
                RaftRPC rpc = new RaftRPC(currentServerId, state.getCurrentTerm(), 
                                        currentServerId, lastEntry.getIndex(), lastEntry.getTerm());
                
                rpcService.sendRPC(serverId, rpc).thenAccept(response -> {
                    if (response != null) {
                        // Responses are processed as incoming RPCs
                        handleRPC(response);
                    }
                });
                metrics.incrementVotesRequested();
            }
        }
        
        logger.info("Started election for term {}", state.getCurrentTerm());
    }
    
    private void checkCommitIndex() {
        if (!state.isLeader()) return;
        
        // Find highest index where majority of servers have matching entries
        int newCommitIndex = state.getCommitIndex();
        
        for (int i = state.getLog().getLastIndex(); i > state.getCommitIndex(); i--) {
            int count = 1; // Leader has it
            
            for (int j = 0; j < clusterSize; j++) {
                if (j != serverIndexMap.get(currentServerId) && state.getMatchIndex(j) >= i) {
                    count++;
                }
            }
            
            if (count > clusterSize / 2 && state.getLog().getEntry(i).getTerm() == state.getCurrentTerm()) {
                newCommitIndex = i;
                break;
            }
        }
        
        if (newCommitIndex > state.getCommitIndex()) {
            state.setCommitIndex(newCommitIndex);
            logger.info("✅ AGREEMENT REACHED: Majority consensus achieved for index {}. Committing changes...", newCommitIndex);
        }
    }
    
    private void applyCommittedEntries() {
        while (state.getLastApplied() < state.getCommitIndex()) {
            int index = state.getLastApplied() + 1;
            RaftLogEntry entry = state.getLog().getEntry(index);
            
            if (entry != null) {
                stateMachine.apply(entry);
                state.setLastApplied(index);
                logger.info("📝 CONSENSUS APPLIED: Command '{}' executed at index {}", entry.getCommand(), index);
            } else {
                break;
            }
        }
    }
    
    // Getters for monitoring
    
    public RaftNodeState getState() {
        return state;
    }
    
    public RaftMetrics getMetrics() {
        return metrics;
    }
    
    public boolean isHealthy() {
        return !state.isElectionTimeoutExpired() || state.isLeader();
    }
}
