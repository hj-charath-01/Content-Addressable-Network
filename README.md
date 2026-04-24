# CAN Network Simulator

This is a simulation of a Content Addressable Network (CAN), a type of structured peer-to-peer DHT where nodes carve up a shared coordinate space and route messages purely by geometry.

There are two ways to run it: a Java CLI you interact with via a menu, and a self-contained HTML file you can just open in a browser.

---

## Background

In a CAN, the keyspace is a *d*-dimensional unit hypercube `[0,1)^d`. Each node owns a rectangular slice of that space (its "zone"). When you store a key, it gets hashed to a coordinate and forwarded, hop by hop, to whichever node's zone contains that coordinate. Lookup works the same way in reverse.

The appeal is that routing tables stay small (each node only needs to know its immediate geometric neighbours), and the whole thing scales gracefully as nodes join and leave.

---

## Project layout

```
.
├── CANWeb.html                         # the browser version (just open it)
├── run.sh                              # compiles and runs the Java CLI
└── src/main/java/com/can/
    ├── core/
    │   ├── CANNetwork.java             # top-level API: bootstrap, add nodes, store, lookup
    │   ├── CANNode.java                # where the actual protocol logic lives
    │   ├── Zone.java                   # hyper-rectangle with split, merge, adjacency checks
    │   ├── Point.java                  # a coordinate in d-dimensional space
    │   └── NodeInfo.java               # what a node knows about each of its neighbours
    ├── network/
    │   └── Message.java                # all the message types nodes pass around
    ├── routing/
    │   └── RoutingTable.java           # greedy next-hop selection
    ├── storage/
    │   └── DataStore.java              # per-node KV store, with versioning and TTL
    ├── util/
    │   └── HashUtil.java               # SHA-256 → d-dimensional point mapping
    └── simulation/
        └── CANSimulation.java          # the interactive CLI entry point
```

---

## Running the Java CLI

You need Java 11 or later. Then just:

```bash
chmod +x run.sh
./run.sh
```

It compiles everything under `src/` into `out/` and starts the simulation. You'll be asked how many dimensions you want (2 is a good starting point) and what to name the first node. After that you get a menu:

```
Node management
 1  Add a single node
 2  Add multiple nodes (comma list)
 3  Remove (graceful leave) a node

Data operations
 4  Store a key/value pair
 5  Look up a key
 6  Store multiple key/value pairs
 7  Look up multiple keys

Diagnostics
 8  Print network summary
 9  Verify zone partitioning
10  Show per-node metrics
11  Show key → coordinate mapping
12  List all nodes
 0  Quit
```

---

## Running the browser version

Open `CANWeb.html` in any modern browser. No server, no build step, nothing to install.

The left side shows a live 2D zone map. You can hover over any zone to see its bounds, key count, and neighbour count, or click it to select a node and dump its contents to the console. The right side is a colour-coded output log. All the same operations are available through the buttons at the bottom.

One note: the canvas only gives you an accurate picture for 2D networks. Higher-dimensional networks still work correctly, you just can't visualise them as neatly.

---

## How it works

### Hashing

Keys are run through SHA-256. The 32-byte digest is sliced into *d* equal chunks, each normalised to `[0, 1)`, giving a *d*-dimensional coordinate. That coordinate is where the key "lives" in the network.

### Zones

Each node owns a hyper-rectangle. A few things the `Zone` class handles:

- **Splitting** bisects along the longest dimension, which keeps zones roughly square over time
- **Adjacency** checks whether two zones share a complete face (same extents on all axes except one). This is the condition for a clean merge.
- **Distance** computes the minimum Euclidean distance from a point to the zone boundary, used by the router to pick the best next hop

### Joining

When a node wants to join, it picks a random point and routes a join request toward it. The node that owns that point splits its zone in half, hands the upper half to the joiner along with any keys that now belong there, and updates its neighbours. The whole thing is a single routed message.

### Storing and looking up

Both work the same way: hash the key to a coordinate, then greedily forward the request to whichever neighbour's zone is closest to the target. Each hop gets geometrically closer. If the hop count exceeds 50 (which shouldn't happen in a healthy network), the current node handles the request locally rather than looping forever.

### Leaving

When a node leaves gracefully:

1. It finds the adjacent neighbour with the smallest zone, which is the easiest merge partner.
2. It hands over all its data and expands the neighbour's zone to cover the gap.
3. It broadcasts the updated zone info to everyone else so their routing tables stay accurate.
4. It removes itself from the overlay.

If there's no clean merge partner (which can happen with uneven zone shapes), the code tries to distribute the departing zone across multiple face-adjacent neighbours instead. If that also fails, it falls back to dumping data to any surviving node rather than losing it.

### Background tasks

Each node runs a lightweight background thread that does two things: pings neighbours every 5 seconds to detect failures, and sweeps the data store every 30 seconds to evict expired entries. Entries can be stored with an optional TTL; reads also do lazy eviction so stale data doesn't linger.

---

## A quick example

```
Enter number of dimensions: 2
Enter a name for the bootstrap node: root
  Bootstrap node "root" is ready.

Choice: 2
  Node names: harold, john, sameen, fusco
  Added "harold"  zone=[0.000,0.500 x 0.000,0.500]
  Added "john"    zone=[0.500,1.000 x 0.000,0.500]
  Added "sameen"  zone=[0.000,0.500 x 0.500,1.000]
  Added "fusco"   zone=[0.500,1.000 x 0.500,1.000]

Choice: 4
  Key  : glasses
  Value: harold@machine.com
  glasses                 -> sameen      OK

Choice: 5
  Key: glasses
  HIT  "harold@machine.com"

Choice: 9
Checking partitioning... OK
```

After adding four nodes the root has been split four times, each node owns one quadrant, and the partitioning check confirms no gaps or overlaps across 2000 random sample points.
