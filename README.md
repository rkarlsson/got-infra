# GoT

## Overview
This is mainly around gofengine which is the order entry, market data, risk and monitoring framework. All built as microservices on top of aeron.

## OS Requirements - do these first
The following packages needs to be installed on Ubuntu to build the project\
 * sudo apt-get install g++ gdb cmake libbsd-dev libbsd0 default-jdk default-jre libmysqlclient-dev libboost-dev libcurlpp-dev python3-psutil python3-boto3 awscli python3-pip

## Building
- First - checkout the code: `git clone https://github.com/rkarlsson/GoT.git`
- Second - make a build dir and go into it: mkdir build && cd build
- Third - configure: cmake -DBUILD_STATIC=ON -DBUILD_SHARED_LIBS=OFF ..
- Fourth - build: make -j 8

All of the above is in the build.sh in the root dir. So all that is needed is to cd GoT/ && ./build.sh

## Docker
- Todo:
 - give it a generic name so its easier to locate
 - map a local $RESDB_PATH to a location in the image
 - spark usually has memory issues in a docker image, will need to address this
 - disk size is too small looks like..
 - connect to it remotely from ms code

First login rkarlsson to docker (we can make team collab later but that costs 300usd as minimum of 5 members so staying as rkarlsson until we want to expand)
docker login -u robkarlsson

### BASE Image (Usually maintained by robert, no need to build this unless underlying requirements change)
To build the base docker image: 
docker build -f BaseImageDockerfile -t robkarlsson/base-got .

Push the base image to dockerhub so it can be used by the normal Docker image described below:
docker push robkarlsson/base-got

### Normal Docker Image ( This is the one to use for testing/training)
Build the normal docker file which uses the above base image as "FROM":
docker build -t got --no-cache .

The run it
docker run -it --entrypoint bash  sha256:e58b0353e53f36050e1e94ea0d56477b541c52d31761a06d63af0cbf36527c1e

F1 Remote-Containers: Open Folder in Container
 - load from existing docker-compose.yml