package com.indeed.imhotep.shardmanager;

import com.google.common.collect.Lists;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author kenh
 */

class ShardScanWork implements Callable<ShardScanWork.Result> {
    private static final Logger LOGGER = Logger.getLogger(ShardScanWork.class);

    private final RemoteCachingPath datasetDir;
    private final ShardFilter shardFilter;
    private final HostsReloader hostsReloader;
    private final ShardAssigner shardAssigner;
    private final ShardAssignmentInfoDao assignmentInfoDao;

    private ShardScanWork(final RemoteCachingPath datasetDir, final ShardFilter shardFilter,
                          final HostsReloader hostsReloader, final ShardAssigner shardAssigner,
                          final ShardAssignmentInfoDao assignmentInfoDao) {
        this.datasetDir = datasetDir;
        this.shardFilter = shardFilter;
        this.hostsReloader = hostsReloader;
        this.shardAssigner = shardAssigner;
        this.assignmentInfoDao = assignmentInfoDao;
    }

    @Override
    public Result call() throws Exception {
        if (!hostsReloader.isLoadedDataSuccessfullyRecently()) {
            LOGGER.warn("Have not loaded hosts recently, will not try to reassign shards for " + datasetDir);
            return new Result(datasetDir, Collections.<ShardDir>emptyList());
        } else {
            final List<Host> hosts = hostsReloader.getHosts();

            final String dataset = datasetDir.getFileName().toString();

            LOGGER.info("Assigning shards in " + dataset + " for " + hosts.size() + " hosts");
            final List<ShardDir> shards = Lists.newArrayList(new ShardScanner(datasetDir, shardFilter));
            assignmentInfoDao.updateAssignments(dataset, shardAssigner.assign(
                    hosts,
                    dataset,
                    shards
            ));
            LOGGER.info("Assigned all shards for " + dataset);
            return new Result(datasetDir, shards);
        }
    }

    static class Result {
        private final RemoteCachingPath datasetDir;
        private final List<ShardDir> shards;

        Result(final RemoteCachingPath datasetDir, final List<ShardDir> shards) {
            this.datasetDir = datasetDir;
            this.shards = shards;
        }

        RemoteCachingPath getDatasetDir() {
            return datasetDir;
        }

        List<ShardDir> getShards() {
            return shards;
        }
    }

    static class Builder {
        private RemoteCachingPath datasetDir;
        private ShardFilter shardFilter;
        private HostsReloader hostsReloader;
        private ShardAssigner shardAssigner;
        private ShardAssignmentInfoDao assignmentInfoDao;

        Builder setDatasetDir(final RemoteCachingPath datasetDir) {
            this.datasetDir = datasetDir;
            return this;
        }

        Builder setShardFilter(final ShardFilter shardFilter) {
            this.shardFilter = shardFilter;
            return this;
        }

        Builder setHostsReloader(final HostsReloader hostsReloader) {
            this.hostsReloader = hostsReloader;
            return this;
        }

        Builder setAssignmentInfoDao(final ShardAssignmentInfoDao assignmentInfoDao) {
            this.assignmentInfoDao = assignmentInfoDao;
            return this;
        }

        Builder setShardAssigner(final ShardAssigner shardAssigner) {
            this.shardAssigner = shardAssigner;
            return this;
        }

        ShardScanWork build() {
            return new ShardScanWork(datasetDir, shardFilter,
                    hostsReloader, shardAssigner, assignmentInfoDao);
        }
    }
}
