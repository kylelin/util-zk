### Election
Out of box election implementations are listed as following.
- `NaiveElect`
- `ContentionFreeElect`

### API
- `Elect` interface details.

 Method        | Type        | Description  
 ------------- |-------------| -----
 participate | sync | join a election 
 update      | async      | invoked by election state change 
 fetchCandidates | sync | get participants of election
 fetchElectionState | sync | check zookeeper ensemble for election state

- Election state

 State        | Description    
 ------------- |-------------
 NOELECTION(0) | no election can be found 
 VOTING(1) | no leader exists and voting for it
 VOTED(2)  | leader exists 
 LEADING(3) | current thread is leader
 LEADED(4) | current thread is follower
 LOSTCONNECTION(-2) | lost zookeeper connection
 LOSTELECTION(-1) | requires a re-participate to election

- Election state transition diagram

![state transition diagram][election-STD]

### Usage
* naive election

```
// add election function to your class
class TestNaiveElect extends NaiveElect {
    @Override
    public Object update(ElectState stat) {
        // TODO: check election state and do your work
    }
}

// join a election
TestNaiveElect test = new TestNaiveElect();
// return current election state 
ElectState state = test.paritcipate();
```

* user defined election

```
// implement your election
Class TestElect implements Elect<Object> {
    @Override
    ElectState participate(ZooKeeperConnection zkConnection, boolean isStartElection) throws KeeperException, InterruptedException; {
        // TODO: join a election
    }

    @Override
    public Object update(ElectState stat) {
        // TODO: check election state and do your work
    }
}
```

### Difference of elections

Naive      | Contention free
 ------------- |-------------
 ![naive] | ![contention free] 
 

[election-STD]:img/ELECTION.jpg
[naive]:img/naive.png
[contention free]:img/ctf.png
