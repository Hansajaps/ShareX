package com.sharex.time;

import com.sharex.replication.ClusterConfig;
import com.sharex.replication.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service to maintain a synchronized logical clock across the cluster.
 * Implements the Berkeley Algorithm for distributed time synchronization.
 * 
 * 1. Leader (Time Coordinator) polls all followers for their physical times.
 * 2. Leader calculates the cluster-wide average physical time.
 * 3. Leader sends relative correction offsets to each follower.
 * 4. Each node maintains an internal offset for its "logical clock".
 */
@Service
public class TimeSyncService {
    private static final Logger logger = LoggerFactory.getLogger(TimeSyncService.class);
    
    // Internal offset added to System.currentTimeMillis()
    private final AtomicLong logicalOffset = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    @Autowired
    @Lazy
    private ClusterConfig clusterConfig;
    
    private static final long SYNC_INTERVAL_SEC = 30;
    private static final int HTTP_TIMEOUT_MS = 5000;

    /**
     * Get the current synchronized logical time.
     * All system operations (logs, metadata) should use this instead of System.currentTimeMillis().
     */
    public long getCurrentTime() {
        return System.currentTimeMillis() + logicalOffset.get();
    }

    /**
     * Updates the local logical offset.
     */
    public void setOffset(long offset) {
        logger.info("🕒 Logical clock adjustment: {}ms offset applied", offset);
        this.logicalOffset.set(offset);
    }

    /**
     * Triggered when this node becomes the Leader via ZooKeeper.
     * Starts the periodic Berkeley synchronization process.
     */
    public void startPeriodicSync() {
        logger.info("🎯 Starting Berkeley Time Synchronization coordinator...");
        // Start sync immediately and then every 30 seconds
        scheduler.scheduleAtFixedRate(this::performSyncRound, 0, SYNC_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    /**
     * Core Berkeley Algorithm Implementation
     */
    public void performSyncRound() {
        // Double check leadership
        if (!clusterConfig.isCurrentServerLeader()) {
            return;
        }

        logger.info("⏱️ Initiating Berkeley Sync Round...");
        
        List<FollowerTimeRecord> records = new ArrayList<>();
        long leaderPhysicalTime = System.currentTimeMillis();
        long sumOfRelativeOffsets = 0; // Relative to coordinator's time
        int count = 1; // Including the coordinator itself

        // 1. Collect physical times from all available servers
        for (ServerConfig follower : clusterConfig.getFollowers()) {
            try {
                long collectionStart = System.currentTimeMillis();
                Long followerTime = fetchFollowerPhysicalTime(follower);
                long rtt = System.currentTimeMillis() - collectionStart;
                
                if (followerTime != null) {
                    // Refinement: Add half the Round-Trip Time to estimated follower time
                    long adjustedFollowerTime = followerTime + (rtt / 2);
                    long relativeDiff = adjustedFollowerTime - leaderPhysicalTime;
                    
                    records.add(new FollowerTimeRecord(follower, relativeDiff));
                    sumOfRelativeOffsets += relativeDiff;
                    count++;
                }
            } catch (Exception e) {
                logger.warn("Could not reach server {} for time sync", follower.getServerId());
            }
        }

        if (count == 1) return; // No followers responded

        // 2. Average the relative offsets
        long avgRelativeOffset = sumOfRelativeOffsets / count;
        logger.info("📊 Cluster average offset from leader: {}ms (Nodes: {})", avgRelativeOffset, count);

        // 3. Coordinator adjusts its own offset to the cluster average
        setOffset(avgRelativeOffset);

        // 4. Send correction offsets to each follower server
        for (FollowerTimeRecord record : records) {
            try {
                // follower_time + correction = leader_time + avgRelativeOffset
                // correction = avgRelativeOffset - relativeDiff
                long correction = avgRelativeOffset - record.relativeOffset;
                sendAdjustmentToFollower(record.server, correction);
            } catch (Exception e) {
                logger.error("Failed to send offset to {}: {}", record.server.getServerId(), e.getMessage());
            }
        }
        
        logger.info("✅ Berkeley Sync Round complete.");
    }

    private Long fetchFollowerPhysicalTime(ServerConfig server) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(server.getBaseUrl() + "/api/time/physical");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(HTTP_TIMEOUT_MS);
            
            if (conn.getResponseCode() == 200) {
                try (Scanner s = new Scanner(conn.getInputStream()).useDelimiter("\\A")) {
                    return Long.parseLong(s.hasNext() ? s.next() : "0");
                }
            }
        } catch (Exception e) {
            logger.debug("Fetch failed for {}: {}", server.getServerId(), e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
        return null;
    }

    private void sendAdjustmentToFollower(ServerConfig server, long offset) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(server.getBaseUrl() + "/api/time/adjust?offset=" + offset);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(HTTP_TIMEOUT_MS);
            conn.getResponseCode(); // Execute
        } catch (Exception e) {
            logger.debug("Adjust failed for {}: {}", server.getServerId(), e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private static class FollowerTimeRecord {
        ServerConfig server;
        long relativeOffset; // difference from coordinator

        FollowerTimeRecord(ServerConfig server, long relativeOffset) {
            this.server = server;
            this.relativeOffset = relativeOffset;
        }
    }
}
