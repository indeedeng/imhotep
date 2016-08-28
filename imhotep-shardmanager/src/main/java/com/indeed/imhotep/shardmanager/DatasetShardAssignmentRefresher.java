package com.indeed.imhotep.shardmanager;

import com.google.common.base.Preconditions;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.log4j.Logger;

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
        Preconditions.checkState(hostsReloader.isLoadedDataSuccessfullyRecently(), "Unable to load latest host list");
        final Future<DataSetScanWork.Result> result = innerRun();
        return result.get();
    }

    private Future<DataSetScanWork.Result> innerRun() {
        final ShardScanWork.Builder shardScanWorkBuilder = new ShardScanWork.Builder()
                .setHostsReloader(hostsReloader)
                .setShardAssigner(shardAssigner)
                .setAssignmentInfoDao(assignmentInfoDao);

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
