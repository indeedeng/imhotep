# JDK home directory
export JAVA_HOME=/usr/java/x64

# directory where imhotep jars are located
export IMHOTEP_LIB_DIR=$HOME/lib/imhotep

# directory where shards (or shard symlinks) are located
export IMHOTEP_SHARD_DIR=/home/imhotep/shards

# directory where imhotep daemon will write logs
export IMHOTEP_LOG_DIR=/imhotep/02/log

# log4j config for imhotep to use
export LOG4J_CONFIG_LOCATION=/home/imhotep/log4j-imhotep-daemon.xml

# opts to be passed to the imhotep daemon JVM
export IMHOTEP_JVM_OPTS="-Xms24G -Xmx24G -XX:NewRatio=8 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$IMHOTEP_LOG_DIR/heapdump.hprof -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$IMHOTEP_LOG_DIR/imhotep-gc.log -Dorg.apache.lucene.FSDirectory.class=com.indeed.common.search.directory.MMapBufferDirectory"

# port that daemon will be listening on
export IMHOTEP_DAEMON_PORT=12345

# memory to allocate in the internal imhotep memory pool in MB
export IMHOTEP_MEMORY_POOL_SIZE=18432

# number of threads to use for the non-FTGS thread pool
export IMHOTEP_NUM_THREADS=48

# zookeeper quorum
export ZK_NODES=aus-imo01.indeed.net:2181,aus-imo03.indeed.net:2181,aus-imo05.indeed.net:2181,aus-imo07.indeed.net:2181,aus-imo09.indeed.net:2181

# directory where shardmanager jars are located
export SQUALL_SHARDMANAGER_LIB_DIR=$HOME/lib/squall-shardmanager

# directory where shardmanager will write logs
export SQUALL_SHARDMANAGER_LOG_DIR=/imhotep/02/log

# opts to be passed to the shardmanager JVM
export SQUALL_SHARDMANAGER_JVM_OPTS="-Xms1G -Xmx1G"

# config to use for super master
export SUPER_MASTER_CONFIG=ImhotepSuperMasterConfig.xml

# config to use for super shard server
export SUPER_SHARD_SERVER_CONFIG=ImhotepSSSConfig-ausprod.xml
