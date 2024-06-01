#!/usr/bin/env bash

cd $(dirname $0)

rm -rf build/plugins/nf-cws*
rm -rf .gradle
make buildPlugins

docker build . -t friedricht/nf-cws-wow
