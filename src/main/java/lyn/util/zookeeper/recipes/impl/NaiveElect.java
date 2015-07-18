package lyn.util.zookeeper.recipes.impl;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect;
import lyn.util.zookeeper.recipes.Elect.ElectState;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yanpeng Lin
 */
public class NaiveElect implements Elect<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NaiveElect.class);

    /* election member name prefix */
    private final String MEMBER_PREFIX = "naive_";

    protected ElectState stat = null;

    public ElectState getStat() {
        return stat;
    }

    public final ElectState fetchElectionState(ZooKeeperConnection zkConnection)
            throws KeeperException, InterruptedException {
        if (zkConnection == null || !zkConnection.isConnected())
            return ElectState.LOSTCONNECTION;
        if (zkConnection.exists(ELECTION, false) == null)
            return ElectState.NOELECTION;
        if (zkConnection.getChildren(ELECTION, false).isEmpty())
            return ElectState.VOTING;
        return ElectState.VOTED;
    }

    public final List<String> fetchCandidates(ZooKeeperConnection zkConnection)
            throws KeeperException, InterruptedException {
        if (zkConnection == null || !zkConnection.isConnected())
            throw new KeeperException.ConnectionLossException();
        if (zkConnection.exists(ELECTION, false) == null)
            throw new KeeperException.DataInconsistencyException();
        List<String> candidates = zkConnection.getChildren(ELECTION, false);
        Collections.sort(candidates, new SequentialComparator());
        return candidates;
    }

    @Override
    public ElectState participate(ZooKeeperConnection zkConnection, boolean isStartElection)
            throws KeeperException, InterruptedException {
        final String MEMBER_PATH_PREFIX = ELECTION + "/" + MEMBER_PREFIX;

        if (zkConnection == null || !zkConnection.isConnected())
            return ElectState.LOSTCONNECTION;

        if (zkConnection.exists(ELECTION, false) == null) {
            if (!isStartElection)
                return ElectState.NOELECTION;
            // initialize election when `isStartElection = true`
            zkConnection.create(ELECTION, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        final String ACTUAL_MEMBER_PATH = zkConnection.create(MEMBER_PATH_PREFIX,
                ByteBuffer.allocate(4).putInt(this.hashCode()).array(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> candidates = fetchCandidates(zkConnection);
        if (isEmpty(candidates))
            return ElectState.LOSTELECTION;

        // leader should exist with smallest sequential id
        final String LEADER_PATH = candidates.get(0);
        if (LEADER_PATH.equalsIgnoreCase(ACTUAL_MEMBER_PATH))
            return ElectState.LEADING;

        Stat watchStat = zkConnection.exists(ELECTION + "/" + LEADER_PATH, new NaiveWatcher(zkConnection,
                ACTUAL_MEMBER_PATH, this));
        LOGGER.debug(ELECTION + "/" + LEADER_PATH + "-->" + watchStat);
        return (watchStat != null) ? ElectState.LEADED : ElectState.LOSTELECTION;
    }

    @Override
    public Object update(ElectState stat) {
        this.stat = stat;
        return stat;
    }

    int parseId(String candidateName) {
        // candidate := /elect/naive_#
        int underBarIndex = candidateName.indexOf('_');
        return Integer.parseInt(candidateName.substring(underBarIndex + 1));
    }

    boolean isEmpty(List<String> list) {
        return list == null || list.isEmpty();
    }

    class SequentialComparator implements Comparator<String> {
        @Override
        public int compare(String candidate1, String candidate2) {
            int candidateId1 = parseId(candidate1);
            int candidateId2 = parseId(candidate2);
            if (candidateId1 < candidateId2)
                return -1;
            if (candidateId1 == candidateId2)
                return 0;
            return 1;
        }
    }

    class NaiveWatcher implements Watcher {
        private WeakReference<ZooKeeperConnection> zkConnectionRef;
        private String memberPath;
        private Elect<Object> elect = null;

        public NaiveWatcher(ZooKeeperConnection zkConnection, final String THIS_MEMBER_PATH, final Elect<Object> elect) {
            this.zkConnectionRef = new WeakReference<ZooKeeperConnection>(zkConnection);
            this.memberPath = THIS_MEMBER_PATH;
            this.elect = elect;
        }

        @Override
        public void process(WatchedEvent event) {
            ElectState state;
            List<String> candidates = null;
            ZooKeeperConnection zkConnection = zkConnectionRef.get();
            try {
                candidates = fetchCandidates(zkConnection);
            } catch (KeeperException | InterruptedException e) {
                elect.update(ElectState.LOSTCONNECTION);
                LOGGER.warn("LOSTCONNECTION when fetching candidates", e);
                return;
            }
            // there's a election but with no candidates
            // which means a re-participating
            LOGGER.debug("candidates: " + candidates);
            if (isEmpty(candidates)) {
                elect.update(ElectState.LOSTELECTION);
                return;
            }

            if (event.getType() == Event.EventType.NodeDeleted) {
                final String LEADER_PATH = candidates.get(0);
                LOGGER.debug("candidates: {}", candidates);

                // if ((ELECTION + "/" + LEADER_PATH).equalsIgnoreCase(this.memberPath)) {
                if (this.memberPath.contains(LEADER_PATH)) {
                    LOGGER.info(Thread.currentThread().getName() + " ## " + event.getType() + " -> " +
                            LEADER_PATH + "--> " + this.memberPath + " ### " + ElectState.LEADING);
                    elect.update(ElectState.LEADING);
                    return;
                } else {
                    // setting watcher on new leader
                    try {
                        Stat watchStat = zkConnection.exists(ELECTION + "/" + LEADER_PATH, new NaiveWatcher(
                                zkConnection, this.memberPath, this.elect));
                        state = (watchStat != null) ? ElectState.LEADED : ElectState.LOSTCONNECTION;

                        LOGGER.info(Thread.currentThread().getName() + " ## " + event.getType() + " -> " +
                                LEADER_PATH + "--> " + this.memberPath + " ### " + state);
                        elect.update(state);
                        return;
                    } catch (KeeperException | InterruptedException e) {
                        LOGGER.warn("LOSTCONNECTION when setting watcher on new leader", e);
                        elect.update(ElectState.LOSTCONNECTION);
                        return;
                    }
                }
            }

            // except lost zk connection and leader node missing
            // pass a lost election state to have a re-participating action
            elect.update(ElectState.LOSTELECTION);
        }
    }
}
