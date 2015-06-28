package lyn.util.zookeeper;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class MiniZooKeeperCluster {
    private final Mock server;
    private final Thread zkServerThread;
    private final String zkNodes;
    private final long zkTimeout;

    public MiniZooKeeperCluster(final String dataDir, final long timeout) throws IOException {
        final int zkPort = getFreePort();
        zkNodes = "localhost:" + zkPort;
        zkTimeout = timeout;

        server = new Mock();
        zkServerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.initializeAndRun(new String[] { Integer.toString(zkPort), dataDir });
                } catch (QuorumPeerConfig.ConfigException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        zkServerThread.setDaemon(true);
        zkServerThread.start();

        awaitForStartup();
    }

    public String getZkNodes() {
        return zkNodes;
    }

    public void shutdown() {
        server.shutdown();
        long startTime = System.currentTimeMillis();
        while (zkServerThread.isAlive() && (System.currentTimeMillis() - startTime) < zkTimeout) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (zkServerThread.isAlive()) {
            throw new RuntimeException("failed to stop zookeeper server");
        }
    }

    private void awaitForStartup() throws IOException {
        final AtomicBoolean connected = new AtomicBoolean(false);
        final ZooKeeper zk = new ZooKeeper(zkNodes, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connected.set(true);
                }
            }
        });
        try {
            long start = System.currentTimeMillis();
            while (!connected.get() && (System.currentTimeMillis() - start) < 30000) {
            }
            if (!connected.get()) {
                throw new RuntimeException("failed to connect to zookeeper");
            }
        } finally {
            // try to shutdown client if connecting failed
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static int getFreePort() throws IOException {
        final ServerSocket socket = new ServerSocket(0);
        final int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    private static class Mock extends ZooKeeperServerMain {
        @Override
        public void initializeAndRun(String[] args) throws QuorumPeerConfig.ConfigException, IOException {
            super.initializeAndRun(args);
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
