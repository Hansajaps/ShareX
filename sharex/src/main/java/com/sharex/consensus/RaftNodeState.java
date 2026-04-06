package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the persistent state of a Raft node.
 * This state must survive crashes and restarts.
 */
@Component
public class RaftNodeState {
    private static final Logger logger = LoggerFactory.getLogger(RaftNodeState.class);
    
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private final AtomicReference<String> votedFor = new AtomicReference<>(null);
    private final RaftLog log;
    
    private com.sharex.zookeeper.ZooKeeperService zooKeeperService;
    
    // Volatile state (doesn't need to be persisted)
    private final AtomicReference<RaftState> state = new AtomicReference<>(RaftState.FOLLOWER);
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);
    private final AtomicInteger commitIndex = new AtomicInteger(0);
    private final AtomicInteger lastApplied = new AtomicInteger(0);
    
    // Leader state (only valid for leader)
    private final AtomicInteger[] nextIndex;
    private final AtomicInteger[] matchIndex;
    
    // Election timing
    private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong electionTimeout = new AtomicLong(0);
    
    private String currentServerId;
    
    public RaftNodeState(RaftLog log, int clusterSize, com.sharex.zookeeper.ZooKeeperService zooKeeperService) {
        this.log = log;
        this.zooKeeperService = zooKeeperService;
        this.nextIndex = new AtomicInteger[clusterSize];
        this.matchIndex = new AtomicInteger[clusterSize];
        
        // Initialize leader state arrays
        for (int i = 0; i < clusterSize; i++) {
            nextIndex[i] = new AtomicInteger(1); // Start from 1 (after dummy entry)
            matchIndex[i] = new AtomicInteger(0);
        }
        
        // Set random election timeout between 500-1000ms
        resetElectionTimeout();
        logger.info("Initialized RaftNodeState for cluster of {} nodes", clusterSize);
    }
    
    public void loadInitialState() {
        if (zooKeeperService != null && currentServerId != null) {
            String state = zooKeeperService.getRaftState(currentServerId);
            if (state != null) {
                String[] parts = state.split("\\|");
                if (parts.length >= 2) {
                    int term = Integer.parseInt(parts[0]);
                    String voted = parts[1].equals("null") ? null : parts[1];
                    this.currentTerm.set(term);
                    this.votedFor.set(voted);
                    logger.info("Loaded Raft state from ZooKeeper: term={}, votedFor={}", term, voted);
                }
            }
        }
    }
    
    public String getCurrentServerId() {
        return currentServerId;
    }
    
    public void setCurrentServerId(String serverId) {
        this.currentServerId = serverId;
    }
    
    // Persistent state methods
    
    public int getCurrentTerm() {
        return currentTerm.get();
    }
    
    public void setCurrentTerm(int term) {
        int oldTerm = currentTerm.getAndSet(term);
        if (oldTerm != term) {
            logger.info("Term changed from {} to {}", oldTerm, term);
            
            // Persist to ZooKeeper
            if (zooKeeperService != null && currentServerId != null) {
                zooKeeperService.saveRaftState(currentServerId, term, votedFor.get());
            }

            // When term changes, votedFor should be reset
            if (term > oldTerm) {
                votedFor.set(null);
            }
        }
    }
    
    public String getVotedFor() {
        return votedFor.get();
    }
    
    public void setVotedFor(String candidateId) {
        String oldVote = votedFor.getAndSet(candidateId);
        if (oldVote != candidateId) {
            logger.info("Voted for {} in term {}", candidateId, currentTerm.get());
            
            // Persist to ZooKeeper
            if (zooKeeperService != null && currentServerId != null) {
                zooKeeperService.saveRaftState(currentServerId, currentTerm.get(), candidateId);
            }
        }
    }
    
    public RaftLog getLog() {
        return log;
    }
    
    // Volatile state methods
    
    public RaftState getState() {
        return state.get();
    }
    
    public void becomeFollower() {
        RaftState oldState = state.getAndSet(RaftState.FOLLOWER);
        if (oldState != RaftState.FOLLOWER) {
            logger.info("State changed from {} to {}", oldState, RaftState.FOLLOWER);
            resetElectionTimeout();
        }
    }
    
    public String getLeaderId() {
        return leaderId.get();
    }
    
    public void setLeaderId(String id) {
        leaderId.set(id);
    }
    
    public void becomeCandidate() {
        RaftState oldState = state.getAndSet(RaftState.CANDIDATE);
        if (oldState != RaftState.CANDIDATE) {
            logger.info("State changed from {} to {}", oldState, RaftState.CANDIDATE);
        }
        // Start new election
        leaderId.set(null);
        setCurrentTerm(getCurrentTerm() + 1);
        setVotedFor(currentServerId); // Vote for self
        resetElectionTimeout();
    }
    
    public void becomeLeader() {
        RaftState oldState = state.getAndSet(RaftState.LEADER);
        if (oldState != RaftState.LEADER) {
            logger.info("State changed from {} to {}", oldState, RaftState.LEADER);
            // Initialize leader state
            initializeLeaderState();
        }
    }
    
    private void initializeLeaderState() {
        int lastIndex = log.getLastIndex();
        for (int i = 0; i < nextIndex.length; i++) {
            nextIndex[i].set(lastIndex + 1);
            matchIndex[i].set(0);
        }
        logger.info("Initialized leader state with nextIndex = {}", lastIndex + 1);
    }
    
    public int getCommitIndex() {
        return commitIndex.get();
    }
    
    public void setCommitIndex(int index) {
        commitIndex.set(index);
        log.setCommitIndex(index);
    }
    
    public int getLastApplied() {
        return lastApplied.get();
    }
    
    public void setLastApplied(int index) {
        lastApplied.set(index);
        log.setLastApplied(index);
    }
    
    // Leader state methods
    
    public int getNextIndex(int serverIndex) {
        if (serverIndex >= 0 && serverIndex < nextIndex.length) {
            return nextIndex[serverIndex].get();
        }
        return 0;
    }
    
    public void setNextIndex(int serverIndex, int index) {
        if (serverIndex >= 0 && serverIndex < nextIndex.length) {
            nextIndex[serverIndex].set(index);
        }
    }
    
    public int getMatchIndex(int serverIndex) {
        if (serverIndex >= 0 && serverIndex < matchIndex.length) {
            return matchIndex[serverIndex].get();
        }
        return 0;
    }
    
    public void setMatchIndex(int serverIndex, int index) {
        if (serverIndex >= 0 && serverIndex < matchIndex.length) {
            matchIndex[serverIndex].set(index);
        }
    }
    
    // Election timing methods
    
    public void resetElectionTimeout() {
        // Random timeout between 1500-3000ms for more stability in HTTP environment
        long timeout = 1500 + (long)(Math.random() * 1500);
        electionTimeout.set(System.currentTimeMillis() + timeout);
        logger.debug("Reset election timeout to {}ms", timeout);
    }
    
    public boolean isElectionTimeoutExpired() {
        return System.currentTimeMillis() > electionTimeout.get();
    }
    
    public void updateLastHeartbeat() {
        lastHeartbeat.set(System.currentTimeMillis());
    }
    
    public long getTimeSinceLastHeartbeat() {
        return System.currentTimeMillis() - lastHeartbeat.get();
    }
    
    // Utility methods
    
    public boolean isLeader() {
        return state.get() == RaftState.LEADER;
    }
    
    public boolean isCandidate() {
        return state.get() == RaftState.CANDIDATE;
    }
    
    public boolean isFollower() {
        return state.get() == RaftState.FOLLOWER;
    }
    
    @Override
    public String toString() {
        return String.format("RaftNodeState{term=%d, state=%s, leader=%s, votedFor=%s, log=%s}", 
                           currentTerm.get(), state.get(), leaderId.get(), votedFor.get(), log);
    }
}
