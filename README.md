# util-zk

`util-zk` is a zookeeper connection client with some implementation of zookeeper recipes like election.
- `ZooKeeperConnection`, a zookeeper connection client.
- `Elect`, a election interface implemented by `NaiveElect` and `ContentionFreeElect`.

### Version
0.0.1

### API
Not all but some main functions provided by `ZooKeeperConnection` are listed in the following table.

| Method        | Type          | Remark  |
| ------------- |-------------| -----|
| ZooKeeperConnection| Constructor   |  |
| createFullPath     | Method(Sync)  |   |
| createIfNotExist    | Method(Sync)  |   |
| UpdateOrCreate     | Method(Sync)  |   |
| getChildren     | Method(Sync)  |   |
| delete     | Method(Sync)  |   |
| setData     | Method(Sync)  |   |
| transaction     | Method(Sync)  | obtain transaction instance associated with current zookeeper connection |
| create     | Method(Async)  |   |
| getChildren     | Method(Async)  |   |
| delete     | Method(Async)  |   |
| setData     | Method(Async)  |   |
| zebra stripes | Method(Async) |     |

### Usage
* Create and close zookeeper connection

```
// initialization
// throws InterruptedException and IOException
String zkNodes = "192.168.0.2:2181,192.168.0.3:2181,192.168.0.4:2181";
ZooKeeperConnection zkc = new ZooKeeperConnection(zkNodes, 30000);
zkc.connect();

// shutdown connection
// throws InterruptedException
zkc.close();
```

* Create and delete znodes
 
```
zkc.create("/path", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

int nodeVersion = -1;
zkc.delete("/path", nodeVersion);
```

* Transaction
 
```
Transaction txn = zkc.transaction();
ZooKeeperConnection.create(txn, "/path", new byte[0], CreateMode.PERSISTENT);
ZooKeeperConnection.delete(txn, "/path", -1);
// throws InterruptedException, KeeperException when transaction failed
zkc.commit(txn);
```

### Recipes
* [Election]
* [DistributedQueue]
* [DistributedLock]
* [Barrier]

License
----

Apache License 2.0

 [Election]:doc/elect.md
