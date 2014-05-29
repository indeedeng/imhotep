#!/bin/bash

source `dirname $BASH_SOURCE`/imhotep-env.sh

mkdir -p $SQUALL_SHARDMANAGER_LOG_DIR

$JAVA_HOME/bin/java -cp $HADOOP_HOME/conf:`readlink -f $SQUALL_SHARDMANAGER_LIB_DIR`/\* \
    com.indeed.squall.shardmanager.superserver.SuperShardServerMain --configfile $SUPER_SHARD_SERVER_CONFIG \
    >& $SQUALL_SHARDMANAGER_LOG_DIR/supershardserver-${SUPER_SHARD_SERVER_CONFIG}.log &
