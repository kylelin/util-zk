package lyn.util.zookeeper.recipes.impl;
import java.util.concurrent.CountDownLatch;

import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect;
import lyn.util.zookeeper.recipes.Elect.ElectState;

import org.apache.zookeeper.KeeperException;

class ElectCandidateTestable implements Runnable {
    ZooKeeperConnection zkc = null;
    CountDownLatch latch = null;
    Elect<Object> elect = null;

    public ElectCandidateTestable(ZooKeeperConnection zkConnection, Elect<Object> naive, CountDownLatch latch) {
        this.zkc = zkConnection;
        this.latch = latch;
        this.elect = naive;
    }

    @Override
    public void run() {
        try {
            ElectState stat = elect.participate(zkc, false);
            if (stat == ElectState.LEADED || stat == ElectState.LEADING)
                latch.countDown();
            // wait for watcher event
            Thread.sleep(1000);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}