# shirakami - Transaction Engine.

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* build related libararies - on Ubuntu, you can install with following command:

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

available options:
* `-DBUILD_TESTS=OFF`
   * never build test programs
* `-DBUILD_DOCUMENTS=OFF`
   * never build documents by doxygen
* `-DBUILD_SHARED_LIBS=OFF`
   * create static libraries instead of shared libraries
* `-DCMAKE_PREFIX_PATH=<installation directory>`
   * indicate prerequiste installation directory
* `-DFORCE_INSTALL_RPATH=ON`
   * force set RPATH for non-default library paths
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON`
   * use formatting for source files
* for debugging only
  * `-DENABLE_SANITIZER=OFF` 
     * disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON`
     * enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON`
     * enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
* logging method (You can select at most one method.)
  * `-DBUILD_PWAL=ON` 
     * enable parallel write-ahead-logging (default: `OFF`)
     <font color="red">This option will be abolished because we plan to use mainly cpr as logging
      method.</font>
      * `-DPWAL_ENABLE_READ_LOG=ON`
        * enable to log read log with write log to verify whether committed schedule is valid.
  * `-DBUILD_CPR=ON` 
     * enable concurrent prefix recovery (default: `OFF`)
  * `-DBUILD_WITH_RECOVERY=ON` 
     * enable initialization of db from log. (default: `OFF`)
* parameter setting
  * Silo variant
    * `-DKVS_MAX_PARALLEL_THREADS=<max concurrent session size>` 
       * It is a max size of concurrent opening session (by 
  enter command). (default: `500`)
    * `-DPARAM_EPOCH_TIME=<epoch time (ms)>` 
       * Epoch time related with latency of commit (durable) and span of resource 
management. (default: `40`)
  * PWAL
    * `-DKVS_LOG_GC_THRESHOLD=<# operations of group commit in a batch>` 
       * This is one of trigger of group commit. If 
  thread local pwal buffer has log records more than this number, it tries group commit. (default: `1000`)
  * CPR
    * `-DPARAM_CHECKPOINT_REST_EPOCH=<time (ms)>` 
      * The rest time after each checkpoint. (default: `40`)
  
### install 

```sh
ninja install
```

### run tests

```sh
ctest -V
```

### generate documents

```sh
ninja doxygen
```

### code coverage

Run cmake with `-DENABLE_COVERAGE=ON` and run tests.
Dump the coverage information into html files with the following steps:
```
cd build
mkdir gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e ../.local/.*'
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/shirakami-gcovr.html
```
Open gcovr-html/shirakami-gcovr.html to see the coverage report.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
