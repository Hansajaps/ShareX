package com.sharex;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

public class ZKRunner {
    public static void main(String[] args) {
        System.setProperty("log4j2.disable.jmx", "true");
        System.setProperty("zookeeper.admin.enableServer", "false");
        try {
            System.out.println("Starting ZooKeeper with config: " + args[0]);
            QuorumPeerMain.main(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
