#!/bin/sh

wget https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz
tar -xzvf snappy-1.1.3.tar.gz
cd snappy-1.1.3 && ./configure --prefix=/usr && make && sudo make install
