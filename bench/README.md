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
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
```

## Running example
```sh
cd [/path/to/project_root]
cd [/path/to/release_build]/bench
./ycsb

```

## ycsb.cpp
### YCSB of shirakami : Available options for general workloads.
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
- `-record`
  - number of database records.
  - default : `100`
- `-rratio`
  - rate of reads in a transaction.
  - default : `100`
- `-skew`
  - access skew of transaction.
  - default : `0.0`
- `-thread`
  - number of worker threads.
  - default : `1`
- `-val_length`
  - byte size of val.
  - default : `4`

for special workloads
- `-include_long_tx`
  - Whether one of worker threads executes long transactions.
  - default : `false`
- `long_tx_ops`
  - number of operations per a long transactions.
  - default : `50`
 
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

## ycsb_mb_nc.cpp
This benchmark is intended to measure performance when multiple non-conflicting long transactions are being processed.

### Available options

### Example
