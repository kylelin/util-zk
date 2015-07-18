package lyn.util.zookeeper.recipes.impl;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.List;

import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect;

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
public class ContentionFreeElect extends NaiveElect implements Elect<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContentionFreeElect.class);

    /* election member name prefix */
    private final String MEMBER_PREFIX = "ctf_";

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

        // set watcher on prior candidate
        final String PRIOR_PATH = fetchPriorCandidate(candidates, ACTUAL_MEMBER_PATH);
        Stat watchStat = zkConnection.exists(ELECTION + "/" + PRIOR_PATH, new ContentionFreeWatcher(zkConnection,
                ACTUAL_MEMBER_PATH, this));

        LOGGER.debug(ELECTION + "/" + PRIOR_PATH + "-->" + watchStat);
        return (watchStat != null) ? ElectState.LEADED : ElectState.LOSTELECTION;
    }

    @Override
    public Object update(ElectState stat) {
        this.stat = stat;
        return stat;
    }

    /**
     * This method will fetch current candidate 's prior candidate
     * 
     * @param candidates
     * @param currentCandidate
     * @return prior candidate or null when candidates does not contain current
     */
    String fetchPriorCandidate(List<String> candidates, String currentCandidate) {
        String prior = null;
        // if currentCandidate is candidates.get(0), return it.
        if (currentCandidate.contains(candidates.get(0)))
            return candidates.get(0);
        for (int idx = 1; idx < candidates.size(); idx++) {
            if (currentCandidate.contains(candidates.get(idx)))
                prior = candidates.get(idx - 1);
        }
        return prior;
    }

    class ContentionFreeWatcher implements Watcher {
        private WeakReference<ZooKeeperConnection> zkConnectionRef;
        private String memberPath;
        private Elect<Object> elect = null;

        public ContentionFreeWatcher(ZooKeeperConnection zkConnection, final String THIS_MEMBER_PATH,
                final Elect<Object> elect) {
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
                if ((ELECTION + "/" + LEADER_PATH).equalsIgnoreCase(this.memberPath)) {
                    elect.update(ElectState.LEADING);
                    return;
                } else {
                    // setting watcher on prior
                    try {
                        final String PRIOR_PATH = fetchPriorCandidate(candidates, this.memberPath);
                        Stat watchStat = zkConnection.exists(ELECTION + "/" + PRIOR_PATH, new ContentionFreeWatcher(
                                zkConnection, this.memberPath, elect));
                        state = (watchStat != null) ? ElectState.LEADED : ElectState.LOSTCONNECTION;
                        elect.update(state);
                        return;
                    } catch (KeeperException | InterruptedException e) {
                        elect.update(ElectState.LOSTCONNECTION);
                        LOGGER.warn("LOSTCONNECTION when setting watcher on prior", e);
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
