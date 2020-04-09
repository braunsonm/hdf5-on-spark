#!/bin/bash

# go one level up from script location (presumably project root) and load .env
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR/..
source .env

echo
echo "**************************  STARTING HDFS CLUSTER  ****************************"
echo

$HDFS_ROOT/sbin/start-dfs.sh

echo
echo "**************************  STARTING SPARK CLUSTER  ***************************"
echo

$SPARK_ROOT/sbin/start-all.sh

echo
echo "*******************************************************************************"
echo
