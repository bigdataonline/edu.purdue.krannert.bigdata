#!/bin/bash

for codeDir in `find . -name "python" | grep -v "venv" | cut -d/ -f2-`; do
    if [ "x${PYTHONPATH}x" == "xx" ]; then
	export PYTHONPATH=`pwd`/${codeDir}
    else
	export PYTHONPATH=${PYTHONPATH}:`pwd`/${codeDir}
    fi
done

jupyter lab &
