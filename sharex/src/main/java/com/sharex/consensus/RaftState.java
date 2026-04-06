package com.sharex.consensus;

/**
 * Raft node states as per the Raft consensus algorithm.
 */
public enum RaftState {
    /**
     * Leader node - handles all client requests and coordinates log replication.
     */
    LEADER("Leader", "👑"),
    
    /**
     * Follower node - passive, responds to leader requests and votes.
     */
    FOLLOWER("Follower", "🐑"),
    
    /**
     * Candidate node - campaigning to become leader during election.
     */
    CANDIDATE("Candidate", "🗳️");
    
    private final String displayName;
    private final String emoji;
    
    RaftState(String displayName, String emoji) {
        this.displayName = displayName;
        this.emoji = emoji;
    }
    
    public String getDisplayName() {
        return displayName;
    }
    
    public String getEmoji() {
        return emoji;
    }
    
    @Override
    public String toString() {
        return emoji + " " + displayName;
    }
}
