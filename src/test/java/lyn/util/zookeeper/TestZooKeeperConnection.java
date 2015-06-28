package lyn.util.zookeeper;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZooKeeperConnection {
    private MiniZooKeeperCluster zk;
    private String tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = System.getProperty("java.io.tmpdir") + "zk.tmp";
        zk = new MiniZooKeeperCluster(tempDir, 6000);
    }

    @After
    public void tearDown() throws IOException {
        if (zk != null) {
            zk.shutdown();
        }

        Files.walkFileTree(Paths.get(tempDir), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void testConnect() throws IOException, InterruptedException {
        String zkNodes = zk.getZkNodes();
        ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);
        zkc.connect();
        assertSame(ZooKeeper.States.CONNECTED, zkc.getState());
        zkc.close();
    }

    @Test(expected = IOException.class)
    public void testFailConnect() throws IOException, InterruptedException {
        String zkNodes = zk.getZkNodes();
        zk.shutdown();
        zk = null;

        ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 2000);
        zkc.connect();
    }

    @Test
    public void testTransactionCommit() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        Transaction tx = zkc.transaction();
        assertNull(zkc.exists("/zkc", false));
        assertNull(zkc.exists("/tx", false));
        ZooKeeperConnection.create(tx, "/zkc", new byte[] { 1, 2, 3 }, CreateMode.PERSISTENT);
        assertNull(zkc.exists("/zkc", false));
        ZooKeeperConnection.create(tx, "/tx", new byte[] { 4, 5, 6 }, CreateMode.PERSISTENT);
        assertNull(zkc.exists("/tx", false));
        List<OpResult> results = ZooKeeperConnection.commit(tx);
        assertNotNull(zkc.exists("/zkc", false));
        assertNotNull(zkc.exists("/tx", false));
        zkc.close();

        assertEquals("/zkc", ((CreateResult) (results.get(0))).getPath());
        assertEquals("/tx", ((CreateResult) (results.get(1))).getPath());
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testTransactionRollback() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        Transaction tx = zkc.transaction();
        assertNull(zkc.exists("/zkc/test", false));
        assertNull(zkc.exists("/tx", false));
        ZooKeeperConnection.create(tx, "/zkc/test", new byte[] { 1, 2, 3 }, CreateMode.PERSISTENT);
        assertNull(zkc.exists("/zkc", false));
        ZooKeeperConnection.create(tx, "/tx", new byte[] { 4, 5, 6 }, CreateMode.PERSISTENT);
        assertNull(zkc.exists("/tx", false));
        ZooKeeperConnection.commit(tx);
        assertNull(zkc.exists("/zkc", false));
        assertNull(zkc.exists("/tx", false));
        zkc.close();
    }

    @Test
    public void testCreateFullPath() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        ZooKeeperConnection.createFullPath(zkc, "/zkc/test", new byte[0], CreateMode.PERSISTENT);
        try {
            ZooKeeperConnection.createFullPath(zkc, "/zkc/test", new byte[0], CreateMode.PERSISTENT);
            assertTrue(false);
        } catch (KeeperException e) {
            assertSame(e.getClass(), KeeperException.NodeExistsException.class);
        }
        ZooKeeperConnection.createFullPath(zkc, "/zkc/test", new byte[0], CreateMode.PERSISTENT, true);
        zkc.close();
    }

    @Test
    public void testUpdateOrCreate() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        assertNull(zkc.exists("/zkc/test", false));
        ZooKeeperConnection.updateOrCreate(zkc, "/zkc/test", new byte[] { 1, 2, 3 }, CreateMode.PERSISTENT);
        assertArrayEquals(new byte[] { 1, 2, 3 }, zkc.getData("/zkc/test", false, new Stat()));
        ZooKeeperConnection.updateOrCreate(zkc, "/zkc/test", new byte[] { 4, 5, 6 }, CreateMode.PERSISTENT);
        assertArrayEquals(new byte[] { 4, 5, 6 }, zkc.getData("/zkc/test", false, new Stat()));
        zkc.close();
    }

    @Test
    public void testCreateIfNotExists() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        assertNull(zkc.exists("/zkc", false));
        assertTrue(ZooKeeperConnection.createIfNotExists(zkc, "/zkc", new byte[] { 1, 2, 3 }, CreateMode.PERSISTENT));
        assertFalse(ZooKeeperConnection.createIfNotExists(zkc, "/zkc", new byte[] { 4, 5, 6 }, CreateMode.PERSISTENT));
        assertArrayEquals(new byte[] { 1, 2, 3 }, zkc.getData("/zkc", false, new Stat()));
        zkc.close();
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testCreateIfNotExistsNoParent() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        assertNull(zkc.exists("/zkc", false));
        try {
            ZooKeeperConnection.createIfNotExists(zkc, "/zkc/test", new byte[0], CreateMode.PERSISTENT);
        } finally {
            zkc.close();
        }
    }

    @Test
    public void increaseCodeCoverage() throws IOException, InterruptedException, KeeperException {
        ZooKeeperConnection zkc = new ZooKeeperConnection(zk.getZkNodes(), 30000);
        zkc.connect();
        zkc.getSessionId();
        zkc.getSessionPasswd();
        zkc.getSessionTimeout();
        zkc.register(new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
            }
        });
        zkc.create("/zkc", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.getChildren("/zkc", false);
        zkc.getChildren("/zkc", false, new Stat());
        zkc.setData("/zkc", new byte[0], -1);
        zkc.getACL("/zkc", new Stat());
        zkc.setACL("/zkc", ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
        zkc.delete("/zkc", -1);
        zkc.close();
    }
}
