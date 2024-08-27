#!/usr/bin/env bash

# Watches files and rebuilds sphinx site as necessary.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

fswatch -o "$SCRIPT_DIR/index.rst" "$SCRIPT_DIR/conf.py" | xargs -n1 -I{} make -C "$SCRIPT_DIR" html