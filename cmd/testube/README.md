# testube
testube is a test tool that takes input via stdin to generate transactions and propose them to the
network.

### Usage

**Start node 1:**
```
testube
```

**Start node 2:**
```
testube -b 12346 -j 12345 -hp 65433
```

In either window type something and press enter.  This will create a transaction which you should
see in the logs of both windows.
