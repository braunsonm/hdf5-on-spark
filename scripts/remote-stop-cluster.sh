#!/bin/bash

# go one level up from script location (presumably project root) and load .env
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR/..
source .env

$TUNNEL_SPEC $REMOTE_PROJECT_ROOT/scripts/stop-cluster.sh
