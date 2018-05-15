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

package com.indeed.imhotep.shardmaster;

import com.google.common.util.concurrent.ListenableFuture;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author kenh
 */

class DatasetShardAssignmentRefresher extends TimerTask {
    private static final Logger LOGGER = Logger.getLogger(DatasetShardAssignmentRefresher.class);
    private final RemoteCachingPath datasetsDir;
    private final ShardFilter shardFilter;
    private final ExecutorService taskExecutorService;
    private final HostsReloader hostsReloader;
    private final ShardAssigner shardAssigner;
    private final ShardAssignmentInfoDao assignmentInfoDao;

    DatasetShardAssignmentRefresher(final RemoteCachingPath datasetsDir,
                                    final ShardFilter shardFilter,
                                    final ExecutorService taskExecutorService,
                                    final HostsReloader hostsReloader,
                                    final ShardAssigner shardAssigner,
                                    final ShardAssignmentInfoDao assignmentInfoDao) {
        this.datasetsDir = datasetsDir;
        this.shardFilter = shardFilter;
        this.taskExecutorService = taskExecutorService;
        this.hostsReloader = hostsReloader;
        this.shardAssigner = shardAssigner;
        this.assignmentInfoDao = assignmentInfoDao;
    }

    DataSetScanWork.Result initialize() throws ExecutionException, InterruptedException {
        if (!hostsReloader.isLoadedDataSuccessfullyRecently()) {
            LOGGER.warn("Unable to load latest host list. Skipping shard assignment refresh");
            return new DataSetScanWork.Result(Collections.<String, ListenableFuture<ShardScanWork.Result>>emptyMap());
        }
        final Future<DataSetScanWork.Result> result = innerRun();
        return result.get();
    }

    private Future<DataSetScanWork.Result> innerRun() {
        final ShardScanWork.Builder shardScanWorkBuilder = new ShardScanWork.Builder(
                hostsReloader,
                shardAssigner,
                assignmentInfoDao
        );

        LOGGER.info("Refreshing all index datasets for assignments");

        return taskExecutorService.submit(
                new DataSetScanWork(datasetsDir, shardFilter, taskExecutorService, shardScanWorkBuilder)
        );
    }

    @Override
    public void run() {
        innerRun();
    }
}