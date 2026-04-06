package com.sharex.consensus;

import java.io.Serializable;
import java.util.Objects;

/**
 * RPC messages used in Raft consensus algorithm.
 * These messages are exchanged between nodes for leader election and log replication.
 */
public class RaftRPC implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public enum RPCType {
        REQUEST_VOTE,
        APPEND_ENTRIES,
        VOTE_RESPONSE,
        APPEND_RESPONSE
    }
    
    private RPCType type;
    private String senderId;
    private int term;
    private String candidateId;  // For RequestVote
    private int lastLogIndex;    // For RequestVote and AppendEntries
    private int lastLogTerm;     // For RequestVote and AppendEntries
    private RaftLogEntry[] entries; // For AppendEntries
    private int leaderCommit;    // For AppendEntries
    private boolean voteGranted; // For VoteResponse
    private boolean success;     // For AppendResponse
    private long timestamp;
    
    // Default constructor for JSON deserialization
    public RaftRPC() {}
    
    // RequestVote RPC
    public RaftRPC(String senderId, int term, String candidateId, int lastLogIndex, int lastLogTerm) {
        this.type = RPCType.REQUEST_VOTE;
        this.senderId = senderId;
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.entries = null;
        this.leaderCommit = 0;
        this.voteGranted = false;
        this.success = false;
        this.timestamp = System.currentTimeMillis();
    }
    
    // AppendEntries RPC
    public RaftRPC(String senderId, int term, int prevLogIndex, int prevLogTerm, 
                   RaftLogEntry[] entries, int leaderCommit) {
        this.type = RPCType.APPEND_ENTRIES;
        this.senderId = senderId;
        this.term = term;
        this.candidateId = null;
        this.lastLogIndex = prevLogIndex;
        this.lastLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
        this.voteGranted = false;
        this.success = false;
        this.timestamp = System.currentTimeMillis();
    }
    
    // VoteResponse RPC
    public RaftRPC(String senderId, int term, boolean voteGranted, String responseType) {
        this.type = RPCType.VOTE_RESPONSE;
        this.senderId = senderId;
        this.term = term;
        this.candidateId = null;
        this.lastLogIndex = 0;
        this.lastLogTerm = 0;
        this.entries = null;
        this.leaderCommit = 0;
        this.voteGranted = voteGranted;
        this.success = false;
        this.timestamp = System.currentTimeMillis();
    }
    
    // AppendResponse RPC
    public RaftRPC(String senderId, int term, boolean success, int responseType) {
        this.type = RPCType.APPEND_RESPONSE;
        this.senderId = senderId;
        this.term = term;
        this.candidateId = null;
        this.lastLogIndex = 0;
        this.lastLogTerm = 0;
        this.entries = null;
        this.leaderCommit = 0;
        this.voteGranted = false;
        this.success = success;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters
    public RPCType getType() { return type; }
    public String getSenderId() { return senderId; }
    public int getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }
    public RaftLogEntry[] getEntries() { return entries; }
    public int getLeaderCommit() { return leaderCommit; }
    public boolean isVoteGranted() { return voteGranted; }
    public boolean isSuccess() { return success; }
    public long getTimestamp() { return timestamp; }
    
    @Override
    public String toString() {
        switch (type) {
            case REQUEST_VOTE:
                return String.format("RequestVote{from=%s, term=%d, candidate=%s, lastLog=%d/%d}", 
                                   senderId, term, candidateId, lastLogIndex, lastLogTerm);
            case APPEND_ENTRIES:
                return String.format("AppendEntries{from=%s, term=%d, prevLog=%d/%d, entries=%d, commit=%d}", 
                                   senderId, term, lastLogIndex, lastLogTerm, 
                                   entries != null ? entries.length : 0, leaderCommit);
            case VOTE_RESPONSE:
                return String.format("VoteResponse{from=%s, term=%d, granted=%s}", 
                                   senderId, term, voteGranted);
            case APPEND_RESPONSE:
                return String.format("AppendResponse{from=%s, term=%d, success=%s}", 
                                   senderId, term, success);
            default:
                return "UnknownRPC";
        }
    }
}
