# difuse [![Build Status](https://travis-ci.org/ipkg/difuse.svg?branch=master)](https://travis-ci.org/ipkg/difuse)

difuse is a new breed of truly distributed multi-purpose datastore.  

Current distributed systems follow a leader/follower model and rely on a fixed leader
for consensus.  This eventually limits the scalability of the cluster.

difuse uses a completely distributed approach in that no node is special and is treated
equally. Each node is the leader for a portion of keys in the cluster.  As nodes join
and leave, keys are re-balanced based across the new set of nodes.  This is accomplished
by using a Distributed Hash Table as the underlying transport.


## Installation
Download the binary from [here].

Start the first node in a terminal window:

```
difused
```

Add subsequent nodes to the cluster in another terminal window (1 for each):

```
difused -a 127.0.0.1:9091 -b 127.0.0.1:4625 -j 127.0.0.1:4624
```

You may need to change the port and/or ip for additional nodes based on the above.

You should now be able to access the HTTP interface on [http://localhost:9090](http://localhost:9090)
or [http://localhost:9091](http://localhost:9091)


## Roadmap

- **v1.0**
    - [ ] Multiple consistency levels.


## Design

### Writes
Difuse has 2 types of writes:

- Non-Transactional
- Transactional

#### Non-Transactional

This type of data does not go through the transaction log.  Data in this category
is considered to be content addressable in order to maintain integrity.  Data is stored
by the hash of its contents.

#### Transactional
Transactional data flows through the transactional log.  Each transaction contains
a key and the hash of the previous transaction.  A transaction is processed as follows:

- Query ring for N successors for K where N is an odd number and K is the key.
- Compute leader vnode for K
- If node does not own the vnode, error out.
- If node owns the vnode, create a new transaction and submit it based on the specified consistency level.
