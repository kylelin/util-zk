package lyn.util.zookeeper.recipes.impl;

import java.util.concurrent.CountDownLatch;

import lyn.util.zookeeper.ZooKeeperConnection;
import lyn.util.zookeeper.recipes.Elect.ElectState;

import org.apache.zookeeper.KeeperException;

class ElectCandidate implements Runnable {
    ZooKeeperConnection zkc = null;
    CountDownLatch latch = null;

    public ElectCandidate(ZooKeeperConnection zkConnection, CountDownLatch latch) {
        this.zkc = zkConnection;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            NaiveElect naive = new NaiveElect();
            ElectState stat = naive.participate(zkc, false);
            if (stat == ElectState.LEADED || stat == ElectState.LEADING)
                latch.countDown();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}