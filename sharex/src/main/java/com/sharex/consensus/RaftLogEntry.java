package com.sharex.consensus;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single entry in the Raft log.
 * Each entry contains a command, term number, and index.
 */
public class RaftLogEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private int index;
    private int term;
    private String command;  // File operation command (e.g., "UPLOAD:filename", "DELETE:filename")
    private String data;     // Additional data (e.g., file metadata)
    private long timestamp;
    
    // Default constructor for JSON deserialization
    public RaftLogEntry() {}
    
    public RaftLogEntry(int index, int term, String command, String data) {
        this.index = index;
        this.term = term;
        this.command = command;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }
    
    public int getIndex() {
        return index;
    }
    
    public int getTerm() {
        return term;
    }
    
    public String getCommand() {
        return command;
    }
    
    public String getData() {
        return data;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("RaftLogEntry{index=%d, term=%d, command='%s', timestamp=%d}", 
                           index, term, command, timestamp);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RaftLogEntry that = (RaftLogEntry) o;
        return index == that.index && term == that.term;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(index, term);
    }
}
