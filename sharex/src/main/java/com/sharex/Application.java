package com.sharex;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.client.RestTemplate;
import com.sharex.server.FileService;
import com.sharex.server.FileServiceImpl;
import com.sharex.replication.ClusterConfig;
import com.sharex.replication.ServerConfig;
import com.sharex.zookeeper.ZooKeeperService;
import com.sharex.time.TimeSyncService;
import com.sharex.consensus.RaftLog;
import com.sharex.consensus.RaftNodeState;
import com.sharex.consensus.RaftNode;
import com.sharex.consensus.RaftRPCService;
import com.sharex.consensus.RaftStateMachine;
import com.sharex.consensus.RaftMetrics;
import com.sharex.consensus.RaftFaultHandler;
import org.springframework.context.annotation.Lazy;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Main application class for ShareX.
 * 
 * This is the entry point for the application.
 * It starts a Spring Boot HTTP server on the specified port.
 * 
 * Usage:
 * java -jar sharex.jar server1 # Starts server1 on port 8081
 * java -jar sharex.jar server2 # Starts server2 on port 8082
 * java -jar sharex.jar server3 # Starts server3 on port 8083
 */
@SpringBootApplication
@EnableScheduling
public class Application {

    private static String currentServerId = "server1"; // Default

    public static void main(String[] args) {
        // Parse command-line argument to determine which server to start
        if (args.length > 0) {
            currentServerId = args[0];
        } else {
            // Default to server1 if no argument provided
            currentServerId = "server1";
        }

        System.out.println("=====================================");
        System.out.println("ShareX - Distributed File Storage");
        System.out.println("Starting server: " + currentServerId);
        System.out.println("=====================================");

        // Set the Spring Boot server port based on the server ID
        // MUST be done AFTER parsing args but BEFORE SpringApplication.run()
        int port = switch (currentServerId) {
            case "server1" -> 8081;
            case "server2" -> 8082;
            case "server3" -> 8083;
            default -> 8080;
        };
        System.setProperty("server.port", String.valueOf(port));
        System.out.println("Configured port: " + port);

        // Configure multipart and post size limits programmatically (resilience)
        System.setProperty("spring.servlet.multipart.max-file-size", "100MB");
        System.setProperty("spring.servlet.multipart.max-request-size", "100MB");
        System.setProperty("server.tomcat.max-http-form-post-size", "100MB");
        System.setProperty("server.tomcat.max-swallow-size", "100MB");

        SpringApplication.run(Application.class, args);
    }

    /**
     * Create and configure the current server instance.
     * 
     * Server Configuration:
     * - server1: Port 8081
     * - server2: Port 8082
     * - server3: Port 8083
     */
    @Bean
    public ServerConfig serverConfig() {
        ServerConfig config = switch (currentServerId) {
            case "server1" -> new ServerConfig(
                    "server1",
                    8081,
                    "localhost",
                    getStoragePath("server1"),
                    false // Role will be determined by ZooKeeper
                );
            case "server2" -> new ServerConfig(
                    "server2",
                    8082,
                    "localhost",
                    getStoragePath("server2"),
                    false // Role will be determined by ZooKeeper
                );
            case "server3" -> new ServerConfig(
                    "server3",
                    8083,
                    "localhost",
                    getStoragePath("server3"),
                    false // Role will be determined by ZooKeeper
                );
            default -> throw new IllegalArgumentException(
                    "Unknown server: " + currentServerId +
                            ". Valid values are: server1, server2, server3");
        };

        System.out.println("Server Configuration: " + config);
        return config;
    }

    /**
     * Create and configure the cluster (list of all servers).
     * This allows each server to know about the other servers.
     */
    @Bean
    public ClusterConfig clusterConfig(ServerConfig currentServerConfig, ZooKeeperService zooKeeperService) {
        List<ServerConfig> allServers = new ArrayList<>();

        // Define all servers in the cluster
        allServers.add(new ServerConfig("server1", 8081, "localhost", getStoragePath("server1"), false));
        allServers.add(new ServerConfig("server2", 8082, "localhost", getStoragePath("server2"), false));
        allServers.add(new ServerConfig("server3", 8083, "localhost", getStoragePath("server3"), false));

        ClusterConfig config = new ClusterConfig(currentServerConfig, allServers, zooKeeperService);
        System.out.println("\nCluster Configuration:");
        System.out.println("  Current Server: " + currentServerConfig.getServerId());
        ServerConfig initialLeader = config.getLeader();
        System.out.println("  Initial Leader: " + (initialLeader != null ? initialLeader.getServerId() : "pending ZooKeeper election..."));
        System.out.println("  Followers: " + config.getFollowers().stream()
                .map(ServerConfig::getServerId)
                .toList());
        System.out.println();

        return config;
    }

    /**
     * Create the file service for storing/retrieving files.
     * This abstraction allows us to swap the storage implementation if needed.
     */
    @Bean
    public FileService fileService(ServerConfig serverConfig) {
        return new FileServiceImpl(serverConfig.getStoragePath());
    }

    /**
     * Create ZooKeeper service for distributed coordination
     */
    @Bean
    public ZooKeeperService zooKeeperService(ServerConfig serverConfig, @Lazy TimeSyncService timeSyncService, @Lazy RaftNodeState raftNodeState) {
        ZooKeeperService.LeaderElectionCallback callback = new ZooKeeperService.LeaderElectionCallback() {
            @Override
            public void onLeadershipGained() {
                System.out.println("🎯 Server " + serverConfig.getServerId() + " has gained leadership!");
                // Begin Berkeley Time Synchronization process
                timeSyncService.startPeriodicSync();
                // Synchronize Raft consensus to match ZooKeeper leadership
                if (raftNodeState != null) {
                    raftNodeState.becomeLeader();
                }
            }

            @Override
            public void onLeadershipLost() {
                System.out.println("💔 Server " + serverConfig.getServerId() + " has lost leadership!");
                if (raftNodeState != null) {
                    raftNodeState.becomeFollower();
                }
            }
        };

        ZooKeeperService service = new ZooKeeperService(serverConfig.getServerId(), serverConfig.getPort(), callback);
        service.start();
        return service;
    }

    /**
     * Configure CORS to allow web UI to access API
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedOrigins("*")
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                        .allowedHeaders("*")
                        .exposedHeaders("Content-Disposition");
            }
        };
    }

    /**
     * RestTemplate for HTTP communication between Raft nodes
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    /**
     * Helper method to get the storage path for a server.
     * Storage directories are created relative to project root.
     */
    private static String getStoragePath(String serverId) {
        // Get the storage directory relative to the current working directory
        String projectRoot = System.getProperty("user.dir");
        String storagePath = projectRoot + File.separator + "storage" + File.separator + serverId;

        // Create directory if it doesn't exist
        File storageDir = new File(storagePath);
        if (!storageDir.exists()) {
            if (storageDir.mkdirs()) {
                System.out.println("Created storage directory: " + storagePath);
            }
        }

        return storagePath;
    }

    // ==================== Raft Consensus Beans ====================
    
    /**
     * Create Raft log for consensus operations.
     */
    @Bean
    public RaftLog raftLog() {
        return new RaftLog();
    }
    
    /**
     * Create Raft node state manager.
     */
    @Bean
    public RaftNodeState raftNodeState(RaftLog raftLog, ZooKeeperService zooKeeperService) {
        RaftNodeState state = new RaftNodeState(raftLog, 3, zooKeeperService); // 3-node cluster
        state.setCurrentServerId(currentServerId);
        state.loadInitialState();
        return state;
    }
    
    /**
     * Create Raft RPC service for inter-node communication.
     */
    @Bean
    public RaftRPCService raftRPCService() {
        return new RaftRPCService();
    }
    
    /**
     * Create Raft state machine for applying committed operations.
     */
    @Bean
    public RaftStateMachine raftStateMachine() {
        return new RaftStateMachine();
    }
    
    /**
     * Create Raft metrics collector.
     */
    @Bean
    public RaftMetrics raftMetrics() {
        return new RaftMetrics();
    }
    
    /**
     * Create Raft fault handler.
     */
    @Bean
    @Lazy
    public RaftFaultHandler raftFaultHandler(@Lazy RaftNode raftNode, RaftMetrics raftMetrics) {
        return new RaftFaultHandler(raftNode, raftMetrics);
    }
    
    /**
     * Create main Raft node.
     */
    @Bean
    public RaftNode raftNode(RaftNodeState raftNodeState, RaftRPCService raftRPCService, 
                            RaftStateMachine raftStateMachine, RaftMetrics raftMetrics) {
        RaftNode node = new RaftNode(raftNodeState, raftRPCService, raftStateMachine, raftMetrics);
        node.setCurrentServerId(currentServerId);
        return node;
    }

}
