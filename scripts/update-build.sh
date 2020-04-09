#!/bin/bash

# go one level up from script location (presumably project root) and load .env
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR/..
source .env

git fetch
git checkout $BRANCH
git pull origin $BRANCH

export PATH=$JAVA_HOME/bin/:$PATH
sbt clean assembly
