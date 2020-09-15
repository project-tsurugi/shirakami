# shirakami - Transaction Engine.

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* build related libararies - on Ubuntu, you can install with following command:

```
apt update -y && apt install -y git build-essential cmake ninja-build doxygen libgflags-dev libgoogle-glog-dev gnuplot libboost-filesystem-dev clang-tidy-8 gcovr
```

```sh
# retrieve third party modules
git submodule update --init --recursive
# If you use -DBUILD_INDEX_KOHLER_MASSTREE(kohler masstree as index structure), it builds third_party/masstree-beta
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
* index (data) structure options
  * default : `-DBUILD_INDEX_YAKUSHIMA=ON` : yakushima is upward compatible with kohler masstree.
  * `-DBUILD_INDEX_KOHLER_MASSTREE=ON -DBUILD_INDEX_YAKUSHIMA=OFF` : use kohler masstree as index (data) structure. 
  If you use this option, execute `[project root]/bootstrap.sh` to build third_party/masstree-beta (kohler masstree).
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DFORMAT_FILES_WITH_CLANG_FORMAT_BEFORE_EACH_BUILD=ON` - use formatting for source files
* `-DWAL=ON` - enable write-ahead-logging
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
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/shirakami-gcovr.html
```
Open gcovr-html/shirakami-gcovr.html to see the coverage report.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
