#!/bin/bash

source `dirname $BASH_SOURCE`/imhotep-env.sh

mkdir -p $IMHOTEP_LOG_DIR

$JAVA_HOME/bin/java -cp `readlink -f $IMHOTEP_LIB_DIR`/\* $IMHOTEP_JVM_OPTS com.indeed.imhotep.service.ImhotepDaemon \
    $IMHOTEP_SHARD_DIR \
    --port $IMHOTEP_DAEMON_PORT \
    --memory $IMHOTEP_MEMORY_POOL_SIZE \
    --threads $IMHOTEP_NUM_THREADS \
    --zknodes $ZK_NODES \
    --log4j $LOG4J_CONFIG_LOCATION \
    >& $IMHOTEP_LOG_DIR/imhotep.out &
