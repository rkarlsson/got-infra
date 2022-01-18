#!/bin/bash

if [ ! -d "./build" ]
then
	mkdir build
fi
cd build
cmake -DBUILD_STATIC=ON -DBUILD_SHARED_LIBS=OFF -DENABLE_PUSH=OFF -DENABLE_COMPRESSION=OFF --parallel 4 ..
