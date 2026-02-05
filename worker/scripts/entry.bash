#!/bin/bash

source /opt/autoware/setup.bash
pushd ../../sbsvf/
just setup > /dev/null 2>&1
popd
python3 -m worker
