#!/usr/bin/env bash

cd $(dirname $0)

sudo rm -rf build/plugins/nf-cws*
sudo rm -rf .gradle
sudo make clean && sudo make buildPlugins && sudo docker build . -t friedricht/nf-cws-ref
