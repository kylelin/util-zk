package lyn.util.zookeeper.recipes;

import java.util.List;

import lyn.util.zookeeper.ZooKeeperConnection;

import org.apache.zookeeper.KeeperException;

/**
 * A class can implement <code>Elect</code> interface when it
 * wants to join leader election as candidate.
 * 
 * @author Yanpeng Lin
 */
public interface Elect<R> {
    final String ELECTION = "/elect";

    public enum ElectState {
        /* Remote States */
        NOELECTION(0),
        VOTING(1),
        VOTED(2),
        LOSTELECTION(-1),
        LOSTCONNECTION(-2),
        /* Local States */
        LEADING(3),
        LEADED(4);

        private final int stateCode;

        ElectState(int stateCode) {
            this.stateCode = stateCode;
        }

        public int getCode() {
            return this.stateCode;
        }

        public String toString() {
            return String.valueOf(this.stateCode);
        }
    }

    /**
     * This method is called when implementation instance
     * want to join a leader election. The participating declaration is
     * one-off activity.
     * 
     * @param zkConnection
     * @param startElection start a new election when no election exists
     * @return The <code>ElectState</code>
     * @throws InterruptedException
     * @throws KeeperException
     */
    ElectState participate(ZooKeeperConnection zkConnection, boolean isStartElection)
            throws KeeperException, InterruptedException;

    /**
     * This method is invoked asynchronously when remote election state changed.
     * 
     * @param stat
     *            <ul>
     *            <li>LEADING, become leader now</li>
     *            <li>LEADED, become follower</li>
     *            <li>LOSTELECTION, need re-participate</li>
     *            <li>LOSTCONNECTION, need re-connection zookeeper</li>
     *            </ul>
     */
    R update(ElectState stat);

    /**
     * @param zkConnection
     * @return a list of current candidates sorted by sequential id or null when failed
     * @throws KeeperException
     * @throws InterruptedException
     */
    List<String> fetchCandidates(ZooKeeperConnection zkConnection)
            throws KeeperException, InterruptedException;

    /**
     * This method is checking remote zookeeper ensemble for election state.
     * 
     * @param zkConnection
     * @return The <code>ElectState</code>:
     *         <ul>
     *         <li>NOELECTION, election znode(/elect) does not exist</li>
     *         <li>VOTING, lost leader and holding a voting</li>
     *         <li>VOTED, leader exists</li>
     *         <li>LOSTCONNECTION</li>
     *         </ul>
     * @throws InterruptedException
     * @throws KeeperException
     */
    ElectState fetchElectionState(ZooKeeperConnection zkConnection)
            throws KeeperException, InterruptedException;
}
