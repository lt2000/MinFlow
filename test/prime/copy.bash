#!/bin/bash
mv ~/minflow/test/prime/parse_yaml_min.py ~/minflow/src/grouping/parse_yaml_min.py
mv ~/minflow/test/prime/workersp.py ~/minflow/src/workflow_manager/workersp.py
mv ~/minflow/test/prime/network.py ~/minflow/src/grouping/network.py

cp ~/minflow/src/grouping/parse_yaml_min.py ~/minflow/test/prime/parse_yaml_min.py 
cp ~/minflow/src/workflow_manager/workersp.py ~/minflow/test/prime/workersp.py 
cp ~/minflow/src/grouping/network.py ~/minflow/test/prime/network.py 
