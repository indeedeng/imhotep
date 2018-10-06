/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.hadoopcommon;

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.TimeUnit;

public class HDFSUtils {
    private static final String HDFS_SOCKET_TIMEOUT = String.valueOf(TimeUnit.SECONDS.toMillis(10));
    private static final String HDFS_SOCKET_TIMEOUT_RETRIES = "1";
    private static final String HDFS_HEDGED_READ_THREADS = "20";
    private static final String HDFS_HEDGED_READ_THRESHOLD_MILLIS = String.valueOf(TimeUnit.SECONDS.toMillis(5));

    public static Configuration getOurHDFSConfiguration() {
        final Configuration hdfsConfiguration = new Configuration();
        hdfsConfiguration.set("dfs.client.socket-timeout", HDFS_SOCKET_TIMEOUT);
        hdfsConfiguration.set("dfs.client.failover.connection.retries.on.timeouts", HDFS_SOCKET_TIMEOUT_RETRIES);
        hdfsConfiguration.set("dfs.client.hedged.read.threadpool.size", HDFS_HEDGED_READ_THREADS);
        hdfsConfiguration.set("dfs.client.hedged.read.threshold.millis", HDFS_HEDGED_READ_THRESHOLD_MILLIS);
        return hdfsConfiguration;
    }
}
