# ZWAN

ZWAN: Leveraging Zone Random Write Area (ZRWA) for Avoiding WAL Tax in LSM Tree

ZWAN is an LSM-tree key-value store for ZNS SSDs that writes WAL records
through the Zone Random Write Area instead of Zone Append. Each record is
committed as soon as it arrives, without group commit, and consecutive
records are packed into the same block by overwriting it in place inside
the ZRWA window. This removes the batching delay of group commit and the
sub-block write amplification of per-record commit at the same time.

ZWAN is built on WALTZ (https://github.com/SNU-ARC/WALTZ), which replaced the
RocksDB WAL path with per-record Zone Append on top of an SPDK-based ZenFS.
The WALTZ WAL path is still available and is used as the baseline in the
evaluation scripts.

# List of changes

plugin/zenfs/fs/wal\_zrwa.cc, wal\_zrwa.h

plugin/zenfs/fs/zbd\_zenfs.cc, io\_zenfs.cc, fs\_zenfs.cc

db/log\_writer.cc, db/log\_reader.cc, db/log\_format.h

db/db\_impl/db\_impl\_open.cc

monitoring/waf\_stats.cc

tools/db\_bench\_tool.cc

The ZRWA WAL engine lives in wal\_zrwa.cc. It hands out ZRWA window slots to
writer threads through a ring with a small priority window so that pages
close to the write pointer are filled first, issues the writes and flushes
on a dedicated I/O thread, and retires a full zone in two steps, a ZRWA
flush followed by a zone finish.

The ZenFS sources handle ZRWA zone allocation and reuse, track the WAL
extent of a file while it is still inside the window, and check at mount
time that the file system was formatted for the WAL mode the process runs
with.

log\_writer.cc sends WAL records to the engine when ZRWA mode is on. Each
WAL page carries a CRC, and log\_reader.cc reads the window back in page
order during recovery and replays the records in sequence order.
db\_impl\_open.cc prints the WAL replay time, its breakdown, and the number
of bytes read from the device during replay.

db\_bench gains the flags below, the benchmarks resetwafstats,
printwafstats\_prefill and printwafstats\_workload that print write
amplification counters, and simulate\_crash\_and\_reopen for recovery tests.

| flag | meaning |
|------|---------|
| --waltz\_wal\_mode=append or zrwa | WAL path: Zone Append (WALTZ) or ZRWA (ZWAN) |
| --waltz\_prio\_window=N | priority ring window in pages, 0 disables it (default 4) |
| --zrwa\_exp\_flush=true or false | explicit ZRWA flush commands, or implicit flush only |
| --zenfs\_skip\_meta\_sync | skip the metadata zone persist on fsync |
| --waltz\_pcie\_addr=BDF | PCIe address of the ZNS SSD |

# Instruction

1. Install SPDK v22.01.2 at the plugin directory

```shell
$ cd plugin
$ git clone https://github.com/spdk/spdk
$ cd spdk
$ git checkout v22.01.2
$ git submodule update --init
$ sudo ./scripts/pkgdep.sh
$ ./configure --with-shared --without-isal
$ make -j4
$ cd ../..
```

2. Install prerequisite library

```shell
$ sudo apt install libgflags-dev
```

3. Build ZWAN

```shell
$ cd script
$ ./rel_build.sh
$ cd ..
```

4. Specify the ZNS SSD

The scripts refer to a device by an alias, samsung or wd. The PCIe BDF
address and the block device name of each alias are kept in the DEV\_PCIE
and DEV\_NAME maps at the top of format\_mkfs.sh, script/bench.sh and
script/recovery\_test.sh. Edit them to match your machine.

The device must support ZRWA. ZWAN was tested on a Samsung PM1731a, whose
ZRWA window is backed by device DRAM, and on a WD ZN540, whose window is
NAND-backed. On the ZN540 the evaluation script runs ZRWA mode with
--zrwa\_exp\_flush=false.

5. Format the device

```shell
$ sudo ./format_mkfs.sh samsung zrwa
```

The second argument selects the WAL mode written into the file system,
append or zrwa. It has to match the --waltz\_wal\_mode given to db\_bench
later; otherwise the mount fails with a WAL mode mismatch error.

6. Run the benchmark scripts

script/test.sh runs the WALTZ and ZenFS comparison from the WALTZ
repository. script/eval.sh runs the evaluation of the ZWAN paper: the
microbenchmarks, the MixGraph benchmarks and the thread and value size
sensitivity runs, for stock ZenFS, WALTZ and ZWAN. It formats the device
before every run and writes a summary table under script/results.

```shell
$ sudo ./script/eval.sh samsung all
```

script/bench.sh is the single-run driver used by both:

```shell
$ cd script
$ sudo ./bench.sh TESTMODE TESTTYPE VALUESIZE KEYRANGE KEYTEST [WALMODE] [THREADS] [DEVICE]
```

TESTMODE 0 enables the WALTZ WAL path and WALMODE picks append or zrwa;
TESTMODE 1 runs stock ZenFS.

7. Run the crash-recovery test

script/recovery\_test.sh formats the device, fills it, kills db\_bench once a
target number of operations has been written, reopens the database and
records the WAL replay time, its phase breakdown and the WAL bytes read
from the device. script/recovery\_sweep.sh repeats it with random targets
and script/recovery\_phase\_aggregate.sh sums up the phase breakdown of a
sweep.

```shell
$ sudo -E env TARGET_OPS=2000000 bash script/recovery_test.sh samsung 0 append zrwa
$ sudo -E bash script/recovery_sweep.sh
$ script/recovery_phase_aggregate.sh script/results/<sweep directory>
```
