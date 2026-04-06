package com.sharex.consensus;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles RPC communication between Raft nodes.
 * Uses HTTP REST for inter-node communication.
 */
@Component
public class RaftRPCService {
    private static final Logger logger = LoggerFactory.getLogger(RaftRPCService.class);
    
    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private com.sharex.zookeeper.ZooKeeperService zooKeeperService;
    
    private final ConcurrentHashMap<String, String> serverUrls = new ConcurrentHashMap<>();
    
    public RaftRPCService() {
        // Initial URLs will be populated dynamically from ZooKeeper
    }
    
    /**
     * Send an RPC message to another server.
     */
    public CompletableFuture<RaftRPC> sendRPC(String targetServerId, RaftRPC rpc) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = serverUrls.get(targetServerId);
                
                // If URL not in cache or we want to ensure it's up to date, check ZooKeeper
                if (url == null && zooKeeperService != null) {
                    String zkData = zooKeeperService.getServerUrl(targetServerId);
                    if (zkData != null) {
                        url = "http://" + zkData;
                        serverUrls.put(targetServerId, url);
                        logger.info("Discovered server {} via ZooKeeper at {}", targetServerId, url);
                    }
                }
                
                // Fallback for local development if ZooKeeper is unavailable
                if (url == null) {
                    int port = switch (targetServerId) {
                        case "server1" -> 8081;
                        case "server2" -> 8082;
                        case "server3" -> 8083;
                        default -> 0;
                    };
                    if (port > 0) {
                        url = "http://localhost:" + port;
                        serverUrls.put(targetServerId, url);
                        logger.info("Using fallback URL for {}: {}", targetServerId, url);
                    }
                }

                if (url == null) {
                    logger.error("Unknown server: {}. ZooKeeper connection: {}", 
                                targetServerId, zooKeeperService != null && zooKeeperService.isConnected());
                    return null;
                }
                
                String endpoint = url + "/raft/rpc";
                logger.info("Sending RPC to {}: {} -> {}", targetServerId, endpoint, rpc);
                
                RaftRPC response = restTemplate.postForObject(endpoint, rpc, RaftRPC.class);
                
                if (response != null) {
                    logger.info("Received RPC response from {}: {}", targetServerId, response);
                } else {
                    logger.warn("No response from {} for RPC: {}", targetServerId, rpc);
                }
                
                return response;
                
            } catch (Exception e) {
                logger.error("Error sending RPC to {}: {}", targetServerId, e.getMessage(), e);
                return null;
            }
        });
    }
    
    /**
     * Send RPC with timeout.
     */
    public CompletableFuture<RaftRPC> sendRPCWithTimeout(String targetServerId, RaftRPC rpc, long timeoutMs) {
        return sendRPC(targetServerId, rpc)
            .orTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
            .exceptionally(throwable -> {
                logger.warn("RPC to {} timed out after {}ms: {}", targetServerId, timeoutMs, rpc.getType());
                return null;
            });
    }
    
    /**
     * Broadcast RPC to all servers except self.
     */
    public CompletableFuture<RaftRPC[]> broadcastRPC(String[] allServers, String currentServerId, RaftRPC rpc) {
        CompletableFuture<RaftRPC>[] futures = new CompletableFuture[allServers.length - 1];
        int index = 0;
        
        for (String serverId : allServers) {
            if (!serverId.equals(currentServerId)) {
                futures[index++] = sendRPC(serverId, rpc);
            }
        }
        
        return CompletableFuture.allOf(futures)
            .thenApply(v -> {
                RaftRPC[] responses = new RaftRPC[futures.length];
                for (int i = 0; i < futures.length; i++) {
                    try {
                        responses[i] = futures[i].get();
                    } catch (Exception e) {
                        responses[i] = null;
                    }
                }
                return responses;
            });
    }
}
