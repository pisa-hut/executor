#!/bin/bash

source /opt/autoware/setup.bash
cd ../../sbsvf/
just setup
cd ../sq/worker/
python3 -m worker
