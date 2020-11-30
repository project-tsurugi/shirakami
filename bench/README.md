# shirakami benchmark
Benchmarking of
- YCSB of shirakami
- third_party/masstree-beta

## Preparation
Please do appropriate build (ex. release-build). If you do benchmarking with
 avoiding contentions against heap, you should use some high performance memory 
 allocator (ex. jemalloc).
 
```
cd [/path/to/project_root]
mkdir build-release
cd build-release
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
```

## Running
```
cd [/path/to/project_root]
cd build-release/bench
LD_PRELOAD=[/path/to/some memory allocator library] ./ycsb
LD_PRELOAD=[/path/to/some memory allocator library] ./masstree
```
YCSB of shirakami : Available options for general workloads.
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

masstree : Available options
- `-cpumhz`
  - number of cpu MHz of execution environment. It is used measuring some time.
  - default : `2000`
- `-duration`
  - duration of benchmark in seconds.
  - default : `1`
- `-instruction`
  - insert or put or get.
  - default : `insert`
- `-key_length`
  - byte size of key.
  - default : `8`
- `-record`
  - number of database records.
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
 
## YCSB Example
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

## masstree Example
- insert benchmark
  - note : default option of -instruction is `insert`
```
LD_PRELOAD=[/path/to/some memory allocator library] ./masstree
```
- put benchmark
```
LD_PRELOAD=[/path/to/some memory allocator library] ./masstree -instruction put
```
- get benchmark
```
LD_PRELOAD=[/path/to/some memory allocator library] ./masstree -instruction get
```
