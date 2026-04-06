package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe Raft log implementation.
 * Manages the replicated log of operations that need consensus.
 */
@Component
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);
    
    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final AtomicInteger commitIndex = new AtomicInteger(0);
    private final AtomicInteger lastApplied = new AtomicInteger(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    // Initialize with dummy entry at index 0 to simplify indexing
    public RaftLog() {
        entries.add(new RaftLogEntry(0, 0, "INIT", "System initialized"));
    }
    
    /**
     * Append a new entry to the log.
     * @param term The term number
     * @param command The command to execute
     * @param data Additional data for the command
     * @return The index of the new entry
     */
    public int appendEntry(int term, String command, String data) {
        lock.writeLock().lock();
        try {
            int index = entries.size();
            RaftLogEntry entry = new RaftLogEntry(index, term, command, data);
            entries.add(entry);
            logger.debug("Appended entry: {}", entry);
            return index;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get an entry at a specific index.
     * @param index The index (1-based)
     * @return The log entry, or null if index doesn't exist
     */
    public RaftLogEntry getEntry(int index) {
        lock.readLock().lock();
        try {
            if (index <= 0 || index >= entries.size()) {
                return null;
            }
            return entries.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the last entry in the log.
     * @return The last log entry
     */
    public RaftLogEntry getLastEntry() {
        lock.readLock().lock();
        try {
            return entries.get(entries.size() - 1);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the last log index.
     * @return The index of the last entry
     */
    public int getLastIndex() {
        lock.readLock().lock();
        try {
            return entries.size() - 1;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the term of the last entry.
     * @return The term of the last entry
     */
    public int getLastTerm() {
        lock.readLock().lock();
        try {
            return getLastEntry().getTerm();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Remove all entries from index onwards.
     * @param index The index to start removing from
     */
    public void truncateFrom(int index) {
        lock.writeLock().lock();
        try {
            if (index >= entries.size()) {
                return;
            }
            
            int removedCount = entries.size() - index;
            entries.subList(index, entries.size()).clear();
            logger.debug("Truncated {} entries from index {}", removedCount, index);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get entries from startIndex to endIndex (inclusive).
     * @param startIndex Start index (1-based)
     * @param endIndex End index (inclusive)
     * @return Array of log entries
     */
    public RaftLogEntry[] getEntries(int startIndex, int endIndex) {
        lock.readLock().lock();
        try {
            if (startIndex <= 0 || startIndex >= entries.size() || endIndex < startIndex) {
                return new RaftLogEntry[0];
            }
            
            int actualEndIndex = Math.min(endIndex, entries.size() - 1);
            List<RaftLogEntry> sublist = entries.subList(startIndex, actualEndIndex + 1);
            return sublist.toArray(new RaftLogEntry[0]);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get all entries from startIndex onwards.
     * @param startIndex Start index (1-based)
     * @return Array of log entries
     */
    public RaftLogEntry[] getEntriesFrom(int startIndex) {
        return getEntries(startIndex, getLastIndex());
    }
    
    /**
     * Update the commit index.
     * @param newCommitIndex New commit index
     */
    public void setCommitIndex(int newCommitIndex) {
        lock.writeLock().lock();
        try {
            int lastIndex = getLastIndex();
            if (newCommitIndex > commitIndex.get() && newCommitIndex <= lastIndex) {
                commitIndex.set(newCommitIndex);
                logger.debug("Updated commit index to {}", newCommitIndex);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get the current commit index.
     * @return The commit index
     */
    public int getCommitIndex() {
        return commitIndex.get();
    }
    
    /**
     * Update the last applied index.
     * @param newLastApplied New last applied index
     */
    public void setLastApplied(int newLastApplied) {
        lastApplied.set(newLastApplied);
    }
    
    /**
     * Get the last applied index.
     * @return The last applied index
     */
    public int getLastApplied() {
        return lastApplied.get();
    }
    
    /**
     * Check if an entry at given index matches the given term.
     * @param index The index to check
     * @param term The expected term
     * @return True if the entry exists and matches the term
     */
    public boolean matchEntry(int index, int term) {
        lock.readLock().lock();
        try {
            RaftLogEntry entry = getEntry(index);
            return entry != null && entry.getTerm() == term;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get total number of entries (including dummy entry at index 0).
     * @return Total number of entries
     */
    public int size() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get all entries for debugging/monitoring.
     * @return Copy of all entries
     */
    public List<RaftLogEntry> getAllEntries() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(entries);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return String.format("RaftLog{size=%d, lastIndex=%d, lastTerm=%d, commitIndex=%d}", 
                               entries.size(), getLastIndex(), getLastTerm(), commitIndex.get());
        } finally {
            lock.readLock().unlock();
        }
    }
}
