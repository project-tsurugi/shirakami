# Shirakami - Transaction Engine.

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* Build related libraries - on Ubuntu, you can install with following command. 
Please check in advance whether ubuntu.deps is correct.:

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

* Shirakami uses Yakushima for in-memory index. So you must install Yakushima for build.

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=/path/to/yakushima/installed ..
ninja
```

Available options:
* `-DBUILD_BENCHMARK=OFF`
   * Never build benchmark programs (default: `ON` )
* `-DBUILD_TESTS=OFF`
   * Never build test programs (default: `ON` )
* `-DBUILD_DOCUMENTS=OFF`
   * Never build documents by doxygen (default: `ON` )
* `-DBUILD_SHARED_LIBS=OFF`
   * Create static libraries instead of shared libraries
* `-DCMAKE_PREFIX_PATH=<installation directory>`
   * Indicate prerequiste installation directory (ex. yakushima)
* `-DFORCE_INSTALL_RPATH=ON`
   * Force set RPATH for non-default library paths
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON`
   * Use formatting for source files
* `-DTSURUGI_FAST_SHUTDOWN=ON`
   * Enable the shutdown process shortcut by default (default: `OFF` )
* For debugging only
  + `-DENABLE_SANITIZER=OFF`
    - Disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug` )

  + `-DENABLE_UB_SANITIZER=ON`
    - Enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON` )

  + `-DENABLE_COVERAGE=ON`
    - Enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug` )

* Logging method (You can select at most one method.)
  + `-DBUILD_PWAL=ON`
    - Enable parallel write-ahead-logging using limestone (default: `ON`)

* Parameter setting
  + Concurrency Control
    - `-DPARAM_EPOCH_TIME=<epoch time[us]>`
      - It is a time of epoch. It is also set at database_options of shirakami::init function. If it is zero, it is set by value of database_options (default is 40000). This setting overrides the database_options setting.
      - Default: `0`.

    - `-DKVS_MAX_PARALLEL_THREADS=<max concurrent session size>`
       * It is a max size of concurrent opening session (by enter command).
       * Default: `112`

    - `-DPARAM_RETRY_READ`
       * The number of retry read without give-up due to conflicts at reading
       record.
       * Default : `0`

## Recommendation

### Setting options

* For high throughput
  + Common workloads
    - `-DKVS_MAX_PARALLEL_THREADS=<max concurrent transaction thread size>`
      - The value of this option is the maximum number of parallel sessions.
      If it is unnecessarily large, the management cost will increase and the
      efficiency will decrease.

  + For high contention workloads
    - `-DPARAM_RETRY_READ=<small num, ex. 0>`
      - When reading fails in the read phase, it is better to return the
      failure without retrying in an environment with high contention. This is
      because in an environment with high contention, the abort rate is high,
      so retries are likely to be wasted work.

  + For low contention workloads
    - `-DPARAM_RETRY_READ=<medium num, ex. 20>`
      - When reading fails in the read phase, you may try until the reading
      succeeds in an environment with low contention. Because, as a result,
      the probability of aborting is small.

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
