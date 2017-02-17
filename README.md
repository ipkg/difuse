# difuse [![Build Status](https://travis-ci.org/ipkg/difuse.svg?branch=master)](https://travis-ci.org/ipkg/difuse)

difuse is a new breed of truly distributed datastores.  It uses gRPC and flatbuffers for
serialization.

**difuse under heavy development and is not yet production ready.**

Current distributed systems follow a leader/follower model and rely on a fixed leader
for consensus. This eventually limits the scalability of the cluster.  difuse contains
multiple leaders through out the cluster.  All nodes perform the same function and are
responsible for a portion of the data.

difuse uses a completely distributed No-One-Special (NOS) approach in that every node in the
cluster is equal and no node is designated to perform anything different or special.  Each
node is the leader for a portion of keys in the cluster.  As nodes join and leave, data is
re-balanced/replicated across the new set of nodes.

## Features

- Each node is of equal weight. No single leader and No-One-Special (NOS).
- Highly-Scalable, fault-tolerant due to the peer-to-peer design
- Data de-duplication


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

    - [ ] Persistent Storage (TBD)
    - [ ] Stability
    - [ ] User defined consistency levels.
        - [ ] Stat
            - [ ] All
            - [x] Leader
            - [ ] Quorum
            - [x] Lazy
        - [ ] SetInode
            - [x] All
            - [x] Leader
            - [ ] Quorum
        - [ ] DeleteInode
            - [x] All
            - [x] Leader
            - [ ] Quorum
        - [ ] DeleteBlock
            - [x] All
            - [ ] Leader
            - [ ] Quorum
            - [ ] Lazy
        - [ ] GetBlock
            - [ ] All
            - [ ] Leader
            - [ ] Quorum
            - [x] Lazy
        - [ ] SetBlock
            - [x] All
            - [ ] Leader
            - [ ] Quorum
            - [ ] Lazy
    - [ ] Replication/Healing
            - [x] Node join replication
            - [ ] Transaction log stagnant drift
    - [ ] Multi-addressable data
        - [x] Content-Addressable
        - [x] Key-Value
        - [ ] File based
        - [ ] Hierarchical
    - [ ] Jepsen tests

- **v1.0+**

    - [ ] Compaction

## Design
This portion outlines the internal architecture and design of difuse.

difuse uses a Chord - a Distributed Hash Table to perform much of it's work.  

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
- If node does not own the vnode, forward/error out.
- If node owns the vnode, create a new transaction and submit it based on the specified consistency level.
