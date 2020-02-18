# kvs - charkey

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* build related libararies - on Ubuntu, you can install with following command:

```
apt update -y && apt install -y git build-essential cmake ninja-build doxygen libgflags-dev libgoogle-glog-dev gnuplot libboost-filesystem-dev
```

```sh
# retrieve third party modules
git submodule update --init --recursive
# build third_party/masstree-beta
./bootstrap.sh
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
* `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries

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
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/kvs_charkey-gcovr.html
```
Open gcovr-html/kvs_charkey-gcovr.html to see the coverage report.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
