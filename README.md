# SCache

SCache is a shuffle cache/daemon integrated with the local Hadoop + Spark trees in this workspace.

The previous (upstream-style) README is archived as `README.old.md`.

## Dependencies (local git repos)

SCache now depends on these repositories being present at the following paths:

- `${HOME}/hadoop/`: [Hadoop](https://github.com/XingZYu/hadoop)
- `${HOME}/spark-3.5/`: [Spark](https://github.com/XingZYu/spark-3.5-scache)
- `${HOME}/spark-apps/`: [Spark Apps](https://github.com/XingZYu/spark-apps)
- `${HOME}/spark-apps/HiBench-7.1.1/`: [HiBench Suite](https://github.com/XingZYu/hibench)

## Build

### Build SCache

```bash
cd ${HOME}/SCache
sbt publishM2   # publishes org.scache to ~/.m2 (needed by spark and hadoop)
sbt assembly    # fat jar for deployment
```

Artifacts:

- `target/scala-2.13/scache_2.13-0.1.0-SNAPSHOT.jar`
- `target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar`

### Build Hadoop

```bash
cd ${HOME}/hadoop
mvn -DskipTests -Pdist -Dtar package
# For faster build
mvn package -T 1C -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true -Denforcer.skip=true
```

### Build Spark 3.5

```bash
cd $HOME/spark-3.5
# Verified 
./build/sbt -Phadoop-3 -Pscala-2.13 package
# Not verified yet
./dev/make-distribution.sh -DskipTests
```

## Deploy / Run

### Start SCache

1. Configure cluster hosts in `conf/slaves` and settings in `conf/scache.conf`.
   - Optional: enable tiered client storage (DRAM → off-heap/CXL → disk) via `scache.storage.tiered.*`.
2. Distribute SCache to the cluster and start it:

```bash
cd $HOME/SCache
sbin/copy-dir.sh
sbin/start-scache.sh
```

Stop:

```bash
cd $HOME/SCache
sbin/stop-scache.sh
```

### Enable in Hadoop MapReduce

- Put `target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar` on the YARN classpath (for example, copy it to `$HADOOP_HOME/share/hadoop/yarn/lib/` on every node).
- Set the following in `$HADOOP_HOME/etc/hadoop/mapred-site.xml`:

```
mapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.MapTask$ScacheOutputBuffer
mapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.ScacheShuffle
mapreduce.scache.home=$HOME/SCache
```

### Enable in Spark

- Make the SCache jar visible to drivers/executors (either copy to `$SPARK_HOME/jars/` or set `spark.scache.jars`).
- Set (for example in `$SPARK_HOME/conf/spark-defaults.conf`):

```
spark.scache.enable true
spark.scache.home $HOME/SCache
spark.scache.jars $HOME/SCache/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar
spark.shuffle.useOldFetchProtocol true
# Optional: bypass Spark shuffle data/index files (store shuffle blocks only in SCache).
# Requires SCache to be available; consider `scache.daemon.putBlock.async=false` in `conf/scache.conf`.
spark.scache.shuffle.noLocalFiles true
```

## IPC Pool Backend (mmap)

SCache can exchange shuffle block bytes between Spark's in-process daemon and the node-local
`ScacheClient` via a single shared `mmap` pool file (offset/len). This is configured via
`scache.daemon.ipc.backend=pool` in `conf/scache.conf` and a pool path such as a DAX-mounted file.

### Tiered Storage (DRAM → off-heap/CXL → disk)

Enable with `scache.storage.tiered.enabled=true` in `conf/scache.conf`. The tier-2 size is
controlled by `scache.memory.offHeap.size`. Optional NUMA binding for tier-2 allocations uses
`scache.memory.offHeap.numaNode` and requires building `native/libscache_numa.so` (see
`scripts/build-numa-native.sh`).

To temporarily disable remote block fetch/replication over TCP, set
`scache.storage.network.enabled=false` (intended for single-host CXL/NUMA experiments).

For a true cross-node “shared CXL” setup backed by a single fsdax-visible file, enable the
master-managed shared pool via `scache.storage.cxl.shared.*` (see `conf/scache.conf`); non-local
consumer shuffle blocks are written directly into the shared pool and reduce hosts read from it
without client-to-client TCP transfers.

### Single-node (multi-executor) shuffle benchmark

For a quick single-machine comparison of **vanilla Spark shuffle** vs **Spark + SCache**, the
`$HOME/spark-apps/` repo in this workspace already provides a shuffle-heavy job:
`org.apache.spark.examples.GroupByTest` (generates random byte arrays and runs `groupByKey`).

Suggested setup for a NUMA experiment (Spark on node 0, SCache pool on node 1):

1. Configure pool IPC in `conf/scache.conf`:
   - `scache.daemon.ipc.backend=pool`
   - `scache.daemon.ipc.pool.path=/dev/shm/scache-ipc.pool` (or a DAX path)
   - `scache.daemon.ipc.pretouch=true` (client touches pages to enforce its NUMA policy)
   - `scache.daemon.ipc.pool.zeroCopy.put=true` (avoid `byte[]` copy when ingesting pool slices)
2. Ensure Spark is actually able to use SCache for block reads/writes:
   - Set `spark.shuffle.useOldFetchProtocol=true` (required by the current SCache integration)
   - Note: older Spark+SCache integration only consulted SCache for **remote** shuffle fetches, so
     single-node runs (often **0 remote blocks**) needed `spark.shuffle.readHostLocalDisk=false`
     to force the remote path. The current integration can also consult SCache for local/host-local
     shuffle blocks when enabled (still requires `spark.shuffle.useOldFetchProtocol=true`), so that
     workaround is no longer required for single-node benchmarks.
3. Start Spark standalone + run the job (see `$HOME/spark-apps/README.md`):
   - Baseline (vanilla shuffle): disable SCache **both** in daemons and Spark config
     (e.g. set `spark.scache.enable=false` in `$HOME/spark-apps/conf/spark-defaults.conf`), then:
     `ENABLE_SCACHE=0 ./start-standalone.sh`
   - SCache (pool on NUMA node 1): `SCACHE_CLIENT_SCRIPT_OPTS="--cpu-node 1 --mem-node 1" ./start-standalone.sh`
   - Use multiple executors on a single worker by setting `EXECUTOR_CORES < CORES_MAX`, e.g.:
     `EXECUTOR_CORES=4 CORES_MAX=32 ./submit-groupbytest.sh 32 800000 1024 32`
4. Compare runtimes / stage shuffle metrics via Spark UI and event logs under
   `$HOME/spark-apps/logs/spark-events/`.

### Self-test (no RPC)

This quick check verifies that two independent `mmap`s of the same pool file observe each other's
writes correctly (i.e., the basic shared-memory mechanism works):

```bash
cd $HOME/SCache
sbt "runMain org.scache.deploy.PoolIpcSelfTest --path /dev/shm/scache-ipc.pool --size 1g --chunk 256m"
```

### Workloads / benchmarks

- Standalone Spark scripts and HiBench in `$HOME/spark-apps/` (see `$HOME/spark-apps/README.md`).
