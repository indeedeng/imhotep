#!/bin/bash

source `dirname $BASH_SOURCE`/imhotep-env.sh

mkdir -p $SQUALL_SHARDMANAGER_LOG_DIR

$JAVA_HOME/bin/java -cp $HADOOP_HOME/conf:`readlink -f $SQUALL_SHARDMANAGER_LIB_DIR`/\* \
    com.indeed.squall.shardmanager.supermaster.SuperMasterMain --springfile $SUPER_MASTER_CONFIG \
    >& $SQUALL_SHARDMANAGER_LOG_DIR/supermaster-${SUPER_MASTER_CONFIG}.log &
