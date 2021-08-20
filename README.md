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
     * Enable parallel write-ahead-logging (default: `OFF` )

        * <font color="red"> This option will be abolished because we plan to use mainly cpr as logging method.
      So this option have a bug because our development hasn't caught up.
      </font>

      * `-DPWAL_ENABLE_READ_LOG=ON`
        * Enable to log read log with write log to verify whether committed schedule is valid.
  + `-DBUILD_CPR=ON`
     * Enable concurrent prefix recovery.
     * Default: `ON`

* Parameter setting
  + Concurrency Control
    - `-DKVS_MAX_PARALLEL_THREADS=<max concurrent session size>`
       * It is a max size of concurrent opening session (by enter command).
       * Default: `500`

    - `-DPARAM_EPOCH_TIME=<epoch time (ms)>`
       * Epoch time related with latency of commit (durable) and span of resource management.
       * Default: `40`

    - `-DPARAM_RETRY_READ`
       * The number of retry read without give-up due to conflicts at reading record.
       * Default : `0`

    - `BUILD_WP=ON`
       * Enable write preserve logic in concurrency control.
       <font color="red"> Note that write preserve logic is developed now, so it is not stable.
       Therefore, default is OFF.
       </font>
       * Default: `OFF`

    - `WP_LEVEL`
       * The level of write preserve.
       The higher level wp is set, the more write preserve logic tracks many infomation.
       * Default: `0`

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

* Benchmarking (project_root/bench)
  + RocksDB
    - `-DBUILD_ROCKSDB_BENCH=ON`
      * Build project_root/bench/rocksdb_bench.
      * Default: `OFF`
        

### Install 

```sh
ninja install
```

### Run tests

```sh
ctest -V
```

### Generate documents

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
