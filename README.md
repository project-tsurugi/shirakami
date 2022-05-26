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
----
BUILD_WP=OFF
  + `-DBUILD_PWAL=ON`
     * Enable parallel write-ahead-logging (default: `OFF` )

        * <font color="red"> This option will be abolished because we plan to
        use mainly cpr as logging method.
      So this option have a bug because our development hasn't caught up.
      </font>

      * `-DPWAL_ENABLE_READ_LOG=ON`
        * Enable to log read log with write log to verify whether committed
        schedule is valid.

  + `-DBUILD_CPR=ON`
     * Enable concurrent prefix recovery.
     * Default: `OFF`
----
BUILD_WP=ON
  + `-DBUILD_PWAL=ON`
    * Enable parallel write-ahead-logging (default: `OFF`)
----
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

    - `BUILD_WP=ON`
       * Enable write preserve logic in concurrency control.
       * Default: `ON`

  + PWAL
    - `-DPARAM_PWAL_LOG_GCOMMIT_THRESHOLD=<# operations of group commit in a batch>`
       * This is one of trigger of group commit.
       If thread local pwal buffer has log records more than this number, it tries group commit.
       * Default: `1000`

  + CPR
    - `-DPARAM_CHECKPOINT_REST_EPOCH=<time (ms)>`
      * The rest time after each checkpoint.
      * Default: `40`
    - `-DPARAM_CPR_DIFF_SET_RESERVE_NUM=<num>`
      * The number of regularly reserving memory for cpr's differences sets.
      If you use for practically and seek high performance, set big number.
      If you set big number and run ctest, it takes so much time, so default is 0.

      * Default: `0`
    - `-DCPR_DIFF_HOPSCOTCH=ON`
      * Use hopscotch hash for cpr's differences sets.
      * Default: `ON`
    - `-DCPR_DIFF_UM=OFF`
      * Use std::unordered_map for cpr's differences sets.
      * Note : This maintenance has not kept up with the latest version.
      * Default: `OFF`
   - `-DCPR_DIFF_VEC=OFF`

      * Use std::vector for cpr's differences sets.
      * Note : This maintenance has not kept up with the latest version.
      * Default: `OFF`

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

    - `-DBUILD_PWAL=OFF -DBUILD_CPR=ON`
      - PWAL generates a log record for each transaction.
      In comparison, CPR, a checkpointing technology, has almost no work until
      the timing of logging comes.

    - `-DPARAM_CHECKPOINT_REST_EPOCH=<large num, ex. 1000>`
      - Checkpointing logging has little work until it's time to log.
    - `-DPARAM_CPR_DIFF_SET_RESERVE_NUM=<large num, ex. 10000>`
      - Performance is good when a large number is reserved so that area
      rearrangement does not occur.

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
  + `-DPARAM_CPR_DIFF_SET_RESERVE_NUM=0`
    - It is faster not to allocate unnecessary memory.
  + `-DPARAM_CHECKPOINT_REST_EPOCH=<large num, ex. 1000>`
    - If you do not enable the option of the fin function that waits for
    logging IO to finish, it is faster to finish the test before logging IO occurs.
    

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
