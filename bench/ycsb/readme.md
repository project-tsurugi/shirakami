# shirakami benchmark
Benchmarking of
- YCSB of shirakami

## Preparation
Please do appropriate build (ex. release-build). If you do benchmarking with
 avoiding contentions against heap, you should use some high performance memory 
 allocator (ex. jemalloc).
 
```sh
cd [/path/to/project_root]
mkdir build-release
cd build-release
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_PWAL=OFF ..
ninja
```

## Running example
```sh
cd [/path/to/release_build]/bench/ycsb
./ycsb

```

## ycsb.cpp
### YCSB of shirakami : Available options for general workloads.
- based on https://github.com/brianfrankcooper/YCSB
- `-cpumhz`
  - number of cpu MHz of execution environment. It is used measuring some time.
  - default : `2000`
- `-duration`
  - duration of benchmark in seconds.
  - default : `1`
- `-key_length`
  - byte size of key.
  - default : `8`
- `-ops`
  - number of operations per a transaction.
  - default : `1`
- `-ops_write_type`
  - type of write operation. update, insert, readmodifywrite.
  - default : `update`
- `-ops_read_type`
  - type of read operation. point, range.
  - default : `point`
- `-record`
  - number of database records before the running phase.
  - default : `100`
- `-rratio`
  - rate of reads in a transaction.
  - default : `100`
- `-scan_length`
  - number of records at scan range.
  - default : `100`
- `-skew`
  - access skew of transaction.
  - default : `0.0`
- `-thread`
  - number of worker threads. If you use transaction_type is long, this is set for 1 at background.
  - default : `1`
- `-transaction_type`
  - type of transaction. short or long or read_only
  - default : `short`
- `-val_length`
  - byte size of val.
  - default : `4`
- `-random_seed`
  - seed for the internal prng
  - default : (using random value from another random number generator)
- `-epoch_duration`
  - epoch duration in microseconds
  - default : (using shirakami default)

### Example
- YCSB-A
  - rratio : `50`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 50
```
- YCSB-B
  - rratio : `95`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 95
```
- YCSB-C
  - rratio : `100`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 100
```

- YCSB-D
  - rratio : `95`
  - record : `1000`
  - ops_write_type : `insert`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 95 -record 1000 -ops_write_type insert 
```

- YCSB-E
  - rratio : `95`
  - record : `100`
  - ops_read_type : `range`
  - ops_write_type : `insert`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 95 -record 100 -ops_write_type insert -ops_read_type range
```


-YCSB-F
  - rratio : `50`
  - ops_write_type : `readmodifywrite`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb -rratio 50 -ops_write_type readmodifywrite 
```
