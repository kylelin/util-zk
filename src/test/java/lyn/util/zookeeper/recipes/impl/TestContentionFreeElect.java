package lyn.util.zookeeper.recipes.impl;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import lyn.util.zookeeper.MiniZooKeeperCluster;
import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect.ElectState;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContentionFreeElect {
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
    public void testFetchPriorCandidate() {
        ContentionFreeElect cfe = new ContentionFreeElect();
        List<String> testCandidates = new ArrayList<>();
        testCandidates.add("cfe_0000000000");
        testCandidates.add("cfe_0000000001");
        testCandidates.add("cfe_0000000002");
        testCandidates.add("cfe_0000000003");
        testCandidates.add("cfe_0000000004");
        testCandidates.add("cfe_0000000005");

        assertEquals("cfe_0000000001", cfe.fetchPriorCandidate(testCandidates, "/elect/cfe_0000000002"));
        assertEquals("cfe_0000000004", cfe.fetchPriorCandidate(testCandidates, "/elect/cfe_0000000005"));
        assertEquals("cfe_0000000000", cfe.fetchPriorCandidate(testCandidates, "/elect/cfe_0000000000"));
        assertEquals(null, cfe.fetchPriorCandidate(testCandidates, "/elect/cfe_0000000006"));
    }

    @Test
    public void testParticipate() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();
        ContentionFreeElect cfe = new ContentionFreeElect();

        // test one local candidate
        ElectState stat = cfe.participate(zkc, true);
        List<String> candidates = cfe.fetchCandidates(zkc);
        assertEquals("ctf_0000000000", candidates.get(0));

        CountDownLatch latch = new CountDownLatch(2);
        // test two more remote candidates
        TestableContentionFreeElect cfe1 = new TestableContentionFreeElect();
        TestableContentionFreeElect cfe2 = new TestableContentionFreeElect();
        ElectCandidateTestable candidate1 = new ElectCandidateTestable(zkc, cfe1, latch);
        ElectCandidateTestable candidate2 = new ElectCandidateTestable(zkc, cfe2, latch);
        new Thread(candidate1).start();
        new Thread(candidate2).start();

        latch.await();
        candidates = cfe.fetchCandidates(zkc);
        assertEquals("ctf_0000000001", candidates.get(1));
        assertEquals("ctf_0000000002", candidates.get(2));

        zkc.close();
    }

    @Test
    public void testParticipateNoConnection() throws IOException, InterruptedException, KeeperException {
        final String zkNodes = zk.getZkNodes();
        final ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);

        zkc.connect();
        // close zookeeper connection
        zkc.close();

        ContentionFreeElect cfe = new ContentionFreeElect();
        ElectState stat = cfe.participate(zkc, false);
        assertEquals(ElectState.LOSTCONNECTION, stat);

        zkc.close();
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

        ContentionFreeElect cfe = new ContentionFreeElect();

        // test one local candidate
        cfe.participate(zkc1, true);
        List<String> candidates = cfe.fetchCandidates(zkc1);
        assertEquals("ctf_0000000000", candidates.get(0));

        CountDownLatch latch = new CountDownLatch(2);
        // test two more remote candidates
        TestableContentionFreeElect cfe1 = new TestableContentionFreeElect();
        TestableContentionFreeElect cfe2 = new TestableContentionFreeElect();
        TestableContentionFreeElect cfe3 = new TestableContentionFreeElect();
        ElectCandidateTestable candidate1 = new ElectCandidateTestable(zkc2, cfe1, latch);
        ElectCandidateTestable candidate2 = new ElectCandidateTestable(zkc3, cfe2, latch);
        ElectCandidateTestable candidate3 = new ElectCandidateTestable(zkc3, cfe3, latch);
        new Thread(candidate1).start();
        new Thread(candidate2).start();
        new Thread(candidate3).start();

        latch.await();
        candidates = cfe.fetchCandidates(zkc2);
        assertEquals("ctf_0000000001", candidates.get(1));
        assertEquals("ctf_0000000002", candidates.get(2));
        zkc1.close();
        // wait until connection completely closed
        while (zkc1.isConnected()) {
            Thread.sleep(1000);
        }

        // candidate set updated
        candidates = cfe.fetchCandidates(zkc2);
        assertEquals("ctf_0000000001", candidates.get(0));
        assertEquals("ctf_0000000002", candidates.get(1));
        final long START_TIMESTAMP_1 = System.currentTimeMillis();
        while (cfe1.getStat() == null && System.currentTimeMillis() - START_TIMESTAMP_1 < 5000) {
            Thread.sleep(1000);
        }
        // check new leader and follower
        assertEquals(ElectState.LEADING.getCode(), cfe1.getStat().getCode());

        zkc2.close();
        // wait until connection completely closed
        while (zkc2.isConnected()) {
            Thread.sleep(1000);
        }

        // candidate set updated
        candidates = cfe.fetchCandidates(zkc3);
        assertEquals("ctf_0000000002", candidates.get(0));
        assertEquals("ctf_0000000003", candidates.get(1));
        // when async watcher event is delayed, the following assertions may fail
        final long START_TIMESTAMP_2 = System.currentTimeMillis();
        while (cfe2.getStat() == null && System.currentTimeMillis() - START_TIMESTAMP_2 < 5000) {
            Thread.sleep(1000);
        }

        // check new leader and follower
        assertEquals(ElectState.LEADING.getCode(), cfe2.getStat().getCode());

        zkc3.close();
    }
}

class TestableContentionFreeElect extends ContentionFreeElect {

    @Override
    public String toString() {
        System.out.println(this.getStat());
        return String.valueOf(this.getStat().getCode());
    }
}
