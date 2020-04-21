# Install masstree
set -e
cd third_party/masstree-beta
./bootstrap.sh
./configure
make clean
make -j CXXFLAGS='-g -W -Wall -O3 -fPIC'
ar cr libjson.a json.o string.o straccum.o str.o msgpack.o clp.o kvrandom.o compiler.o memdebug.o kvthread.o misc.o
ranlib libjson.a
cd ../../

# This directory will be used for logging.
mkdir -p log
