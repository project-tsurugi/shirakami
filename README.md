# shirakami - Transaction Engine.

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* Build related libraries - on Ubuntu, you can install with following command:

```sh
# clone this repository
git clone --recurse-submodules this_repository
cd shirakami
sudo apt update -y && sudo apt install -y $(cat build_tools/ubuntu.deps)
```

## Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y $(cat build_tools/ubuntu.deps)
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

Available options:
* `-DBUILD_TESTS=OFF`
   * Never build test programs
* `-DBUILD_DOCUMENTS=OFF`
   * Never build documents by doxygen
* `-DBUILD_SHARED_LIBS=OFF`
   * Create static libraries instead of shared libraries
* `-DCMAKE_PREFIX_PATH=<installation directory>`
   * Indicate prerequiste installation directory
* `-DFORCE_INSTALL_RPATH=ON`
   * Force set RPATH for non-default library paths
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON`
   * Use formatting for source files
* For debugging only
  + `-DENABLE_SANITIZER=OFF`
    - Disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug` )

  + `-DENABLE_UB_SANITIZER=ON`
    - Enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON` )

  + `-DENABLE_COVERAGE=ON`
    - Enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug` )

* Logging method (You can select at most one method.)
  + `-DBUILD_PWAL=ON`
    * Enable parallel write-ahead-logging using limestone (default: `OFF`)

* Parameter setting
  + Concurrency Control
    - `-DKVS_MAX_PARALLEL_THREADS=<max concurrent session size>`
       * It is a max size of concurrent opening session (by enter command).
       * Default: `112`

    - `-DPARAM_EPOCH_TIME=<epoch time (ms)>`
       * Epoch time related with latency of commit (durable) and span of
       resource management.
       * Default: `40`

    - `-DPARAM_RETRY_READ`
       * The number of retry read without give-up due to conflicts at reading
       record.
       * Default : `0`

* Benchmarking (project_root/bench)
  + RocksDB
    - `-DBUILD_ROCKSDB_BENCH=ON`
      * Build project_root/bench/rocksdb_bench.
      * Default: `OFF`
        

## Recommendation

### Setting options

* For high throughput
  + Common workloads
    - `-DKVS_MAX_PARALLEL_THREADS=<max concurrent transaction thread size>`
      - The value of this option is the maximum number of parallel sessions.
      If it is unnecessarily large, the management cost will increase and the
      efficiency will decrease.

  + For high contention workloads
    - `-DPARAM_EPOCH_TIME=<medium num, ex. 10>`
      - For workloads that repeatedly insert and delete the same primary key,
      new inserts will be rejected until physical unhooking occurs in garbage
      collection, so set a small value here to execute frequently. If this
      value is too small, the cache will be polluted frequently, so make sure
      it is not too small.

    - `-DPARAM_RETRY_READ=<small num, ex. 0>`
      - When reading fails in the read phase, it is better to return the
      failure without retrying in an environment with high contention. This is
      because in an environment with high contention, the abort rate is high,
      so retries are likely to be wasted work.

  + For low contention workloads
    - `-DPARAM_EPOCH_TIME=<large num, ex. 1000>`
      - In the single version concurrency control mode, garbage collection
      basically causes performance degradation, so set a large value here
      and execute it infrequently.

    - `-DPARAM_RETRY_READ=<medium num, ex. 20>`
      - When reading fails in the read phase, you may try until the reading
      succeeds in an environment with low contention. Because, as a result,
      the probability of aborting is small.

* For low latency
  + `-DPARAM_EPOCH_TIME=<small time, ex. 10> -DPARAM_CHECKPOINT_REST_EPOCH=<small time, ex. 10>`
    - Frequent logging improves latency but worsens throughput.
* For fast testing

## Install 

```sh
ninja install
```

## Run tests

```sh
ctest -V
```

## Generate documents

```sh
ninja doxygen
```

### Code coverage

Run cmake with `-DENABLE_COVERAGE=ON` and run tests.
Dump the coverage information into html files with the following steps:

```sh
cd build
mkdir gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e ../.local/.*'
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/shirakami-gcovr.html
```

Open gcovr-html/shirakami-gcovr.html to see the coverage report.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
