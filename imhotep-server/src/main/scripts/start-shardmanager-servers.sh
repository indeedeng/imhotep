#!/bin/bash

source `dirname $BASH_SOURCE`/imhotep-env.sh

mkdir -p $SQUALL_SHARDMANAGER_LOG_DIR

function run_shard_server() {
    local CONFIG=$1
    $JAVA_HOME/bin/java -cp `readlink -f $SQUALL_SHARDMANAGER_LIB_DIR`/\* $SQUALL_SHARDMANAGER_JVM_OPTS \
        com.indeed.squall.shardmanager.ShardServerMain --springfile $CONFIG >& $SQUALL_SHARDMANAGER_LOG_DIR/shard-server-$CONFIG.log &
    echo "started shard server with config $CONFIG"
}

for CONFIG in ${SHARD_SERVER_CONFIGS[@]}; do
    run_shard_server $CONFIG
done
