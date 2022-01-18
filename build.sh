#!/bin/bash

if [ ! -d "./build" ]
then
	mkdir build
	./configure.sh
fi
cd build
make -j 5
