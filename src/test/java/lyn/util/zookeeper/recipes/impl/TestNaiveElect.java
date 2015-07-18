package lyn.util.zookeeper.recipes.impl;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lyn.util.zookeeper.MiniZooKeeperCluster;
import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect;
import lyn.util.zookeeper.recipes.Elect.ElectState;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNaiveElect {
    private MiniZooKeeperCluster zk;
    private String tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = System.getProperty("java.io.tmpdir") + "zk.tmp";
        zk = new MiniZooKeeperCluster(tempDir, 3000);
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

    @Test(expected = NumberFormatException.class)
    public void testParseId() {
        NaiveElect naive = new NaiveElect();
        assertEquals(1, naive.parseId("/elect/naive_1"));
        naive.parseId("/e/n1");
        naive.parseId("/elect/naive_asdf");
        assertEquals(12, naive.parseId("/elect/naive_12"));
    }

    @Test
    public void testFetchElectionState() throws KeeperException, InterruptedException, IOException {
        String zkNodes = zk.getZkNodes();
        ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);
        zkc.connect();
        NaiveElect naive = new NaiveElect();

        // test when ensemble is not initialized
        assertEquals(ElectState.NOELECTION, naive.fetchElectionState(zkc));

        zkc.createIfNotExists("/elect", new byte[0], CreateMode.PERSISTENT);
        // test when voting for leader
        assertEquals(ElectState.VOTING, naive.fetchElectionState(zkc));

        zkc.createIfNotExists("/elect/naive_", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        // test when leader exists
        assertEquals(ElectState.VOTED, naive.fetchElectionState(zkc));

        zkc.close();
        // test when lost connection
        assertEquals(ElectState.LOSTCONNECTION, naive.fetchElectionState(zkc));
    }

    @Test(expected = KeeperException.DataInconsistencyException.class)
    public void testFailFetchCandidates() throws IOException, InterruptedException, KeeperException {
        String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);
        zkc.connect();
        NaiveElect naive = new NaiveElect();

        // test when empty election without candidates
        naive.fetchCandidates(zkc);

        zkc.close();
        assertEquals(null, naive.fetchCandidates(zkc));
    }

    @Test
    public void testFetchCandidates() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();
        NaiveElect naive = new NaiveElect();

        // test one candidate
        zkc.createFullPath("/elect/naive_", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        List<String> candidates = naive.fetchCandidates(zkc);
        assertEquals("naive_0000000000", candidates.get(0));

        // test two more candidates
        zkc.createFullPath("/elect/naive_", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        zkc.createFullPath("/elect/naive_", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        candidates = naive.fetchCandidates(zkc);
        assertEquals("naive_0000000001", candidates.get(1));
        assertEquals("naive_0000000002", candidates.get(2));

        zkc.close();
    }

    @Test
    public void testParticipate() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();
        NaiveElect naive = new NaiveElect();

        // test one local candidate
        naive.participate(zkc, true);
        List<String> candidates = naive.fetchCandidates(zkc);
        assertEquals("naive_0000000000", candidates.get(0));

        CountDownLatch latch = new CountDownLatch(2);
        // test two more remote candidates
        ElectCandidate candidate1 = new ElectCandidate(zkc, latch);
        ElectCandidate candidate2 = new ElectCandidate(zkc, latch);
        new Thread(candidate1).start();
        new Thread(candidate2).start();

        latch.await();
        candidates = naive.fetchCandidates(zkc);
        assertEquals("naive_0000000001", candidates.get(1));
        assertEquals("naive_0000000002", candidates.get(2));

        zkc.close();
    }

    @Test
    public void testParticipateNoElection() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();

        NaiveElect naive = new NaiveElect();
        ElectState stat = naive.participate(zkc, false);
        assertEquals(ElectState.NOELECTION, stat);

        zkc.close();
    }

    @Test
    public void testParticipateNoConnection() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();
        // close zookeeper connection
        zkc.close();

        NaiveElect naive = new NaiveElect();
        ElectState stat = naive.participate(zkc, false);
        assertEquals(ElectState.LOSTCONNECTION, stat);

        zkc.close();
    }

    @Test
    public void testElection() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc1 = new ZooKeeperConnection(zkNodes, 10000);
        final ZooKeeperConnection zkc2 = new ZooKeeperConnection(zkNodes, 10000);
        final ZooKeeperConnection zkc3 = new ZooKeeperConnection(zkNodes, 30000);

        zkc1.connect();
        zkc2.connect();
        zkc3.connect();

        NaiveElect naive = new NaiveElect();

        // test one local candidate
        naive.participate(zkc1, true);
        List<String> candidates = naive.fetchCandidates(zkc1);
        assertEquals("naive_0000000000", candidates.get(0));

        CountDownLatch latch = new CountDownLatch(2);
        // test two more remote candidates
        TestableNaiveElect naive1 = new TestableNaiveElect();
        TestableNaiveElect naive2 = new TestableNaiveElect();
        ElectCandidateTestable candidate1 = new ElectCandidateTestable(zkc2, naive1, latch);
        ElectCandidateTestable candidate2 = new ElectCandidateTestable(zkc3, naive2, latch);
        new Thread(candidate1).start();
        new Thread(candidate2).start();

        latch.await();
        candidates = naive.fetchCandidates(zkc2);
        assertEquals("naive_0000000001", candidates.get(1));
        assertEquals("naive_0000000002", candidates.get(2));

        zkc1.close();
        // wait until connection completely closed
        while (zkc1.isConnected()) {
            Thread.sleep(1000);
        }

        // candidate set updated
        candidates = naive.fetchCandidates(zkc2);
        assertEquals("naive_0000000001", candidates.get(0));
        assertEquals("naive_0000000002", candidates.get(1));

        final long START_TIMESTAMPE = System.currentTimeMillis();
        while ((naive1.getStat() == null || naive2.getStat() == null)
                && System.currentTimeMillis() - START_TIMESTAMPE < 5000) {
            Thread.sleep(1000);
        }
        // check new leader and follower
        assertEquals(ElectState.LEADED.getCode(), naive2.getStat().getCode());
        assertEquals(ElectState.LEADING.getCode(), naive1.getStat().getCode());

        zkc2.close();
        zkc3.close();
    }

    @Test
    public void testPairElection() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc1 = new ZooKeeperConnection(zkNodes, 10000);
        final ZooKeeperConnection zkc2 = new ZooKeeperConnection(zkNodes, 10000);
        final ZooKeeperConnection zkc3 = new ZooKeeperConnection(zkNodes, 30000);

        zkc1.connect();
        zkc2.connect();
        zkc3.connect();

        NaiveElect naive = new NaiveElect();

        // test one local candidate
        naive.participate(zkc1, true);
        List<String> candidates = naive.fetchCandidates(zkc1);
        assertEquals("naive_0000000000", candidates.get(0));

        CountDownLatch latch = new CountDownLatch(2);
        // test two more remote candidates
        TestableNaiveElect naive1 = new TestableNaiveElect();
        TestableNaiveElect naive2 = new TestableNaiveElect();
        TestableNaiveElect naive3 = new TestableNaiveElect();
        ElectCandidateTestable candidate1 = new ElectCandidateTestable(zkc2, naive1, latch);
        ElectCandidateTestable candidate2 = new ElectCandidateTestable(zkc3, naive2, latch);
        ElectCandidateTestable candidate3 = new ElectCandidateTestable(zkc3, naive3, latch);
        new Thread(candidate1).start();
        new Thread(candidate2).start();
        new Thread(candidate3).start();

        latch.await();
        candidates = naive.fetchCandidates(zkc2);
        assertEquals("naive_0000000001", candidates.get(1));
        assertEquals("naive_0000000002", candidates.get(2));

        zkc1.close();
        // wait until connection completely closed
        while (zkc1.isConnected()) {
            Thread.sleep(1000);
        }

        // candidate set updated
        candidates = naive.fetchCandidates(zkc2);
        assertEquals("naive_0000000001", candidates.get(0));
        assertEquals("naive_0000000002", candidates.get(1));
        final long START_TIMESTAMP_1 = System.currentTimeMillis();
        while ((naive1.getStat() == null || naive2.getStat() == null)
                && System.currentTimeMillis() - START_TIMESTAMP_1 < 5000) {
            Thread.sleep(1000);
        }
        // check new leader and follower
        assertEquals(ElectState.LEADED.getCode(), naive2.getStat().getCode());
        assertEquals(ElectState.LEADING.getCode(), naive1.getStat().getCode());

        zkc2.close();
        // wait until connection completely closed
        while (zkc2.isConnected()) {
            Thread.sleep(1000);
        }

        // candidate set updated
        candidates = naive.fetchCandidates(zkc3);
        assertEquals("naive_0000000002", candidates.get(0));
        assertEquals("naive_0000000003", candidates.get(1));
        // when async watcher event is delayed, the following assertions may fail
        final long START_TIMESTAMP_2 = System.currentTimeMillis();
        while ((naive2.getStat() == null || naive3.getStat() == null)
                && System.currentTimeMillis() - START_TIMESTAMP_2 < 5000) {
            Thread.sleep(1000);
        }

        // check new leader and follower
        assertEquals(ElectState.LEADED.getCode(), naive3.getStat().getCode());
        assertEquals(ElectState.LEADING.getCode(), naive2.getStat().getCode());

        zkc3.close();
    }
}

class TestableNaiveElect extends NaiveElect {

    @Override
    public String toString() {
        System.out.println(this.getStat());
        return String.valueOf(this.getStat().getCode());
    }
}