#!/usr/bin/env bash

cd $(dirname $0)

rm -rf build/plugins/nf-cws*
rm -rf .gradle
make buildPlugins && sudo docker build . -t friedricht/nf-cws-wow
