#!/bin/bash

# go one level up from script location (presumably project root) and load .env
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR/..
source .env

echo
echo "**************************  STOPPING HDFS CLUSTER  ****************************"
echo

$HDFS_ROOT/sbin/stop-dfs.sh

echo
echo "**************************  STOPPING SPARK CLUSTER  ***************************"
echo

$SPARK_ROOT/sbin/stop-all.sh

echo
echo "*******************************************************************************"
echo
