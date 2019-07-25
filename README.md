# test_pglz
This is a test suit for benchmarking pglz decompression.

To run benchmarks simply install this extension to your db and execute select ```select test_pglz()```

You will get results table. The results are presented in nanoseconds per byte of decompressed data.


#installation

You can get PostgreSQL sources at http://github.com/postgres/postgres or install it from apt\brew.
To build from sources you will need dependencies
```sudo apt-get install bison flex wget build-essential git gcc make zlib1g-dev libreadline6 libreadline6-dev```
Same libs can be obtained from brew.

Test script here expects that postgresql will be installed in ~/project/bin

To do this you should 
1. Clone PostgreSQL sources to ~/project/pgsql and cd there
2. ./configure --prefix=$HOME/project/ --enable-debug --enable-cassert
3. make -j4 install
4. Clone this repo to ~/project/pgsql/conrib/test_pglz/ and cd there
5. run ./test.sh

Inspired by [optimization](https://habr.com/en/company/yandex/blog/457612/) of Lz4 in [ClickHouse](https://github.com/yandex/ClickHouse/)
