#!/bin/bash

source `dirname $0`/imhotep-env.sh

$JAVA_HOME/bin/java -cp `readlink -f $IMHOTEP_LIB_DIR`/\* com.indeed.imhotep.service.ImhotepDaemon asdf --port $IMHOTEP_DAEMON_PORT --shutdown
