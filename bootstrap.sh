#install masstree
cd third_party/masstree-beta
./bootstrap.sh
./configure
make clean
make -j CXXFLAGS='-g -W -Wall -O3 -fPIC'
