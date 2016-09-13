package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            final Map<String, ShardDir> shardsMap = new HashMap<>();
            for (final ShardDir shardDir : new ShardScanner(datasetDir, shardFilter)) {
                final ShardDir existing = shardsMap.get(shardDir.getId());
                if ((existing == null) || (existing.getVersion() < shardDir.getVersion())) {
                    shardsMap.put(shardDir.getId(), shardDir);
                }
            }

            assignmentInfoDao.updateAssignments(dataset, DateTime.now(), shardAssigner.assign(
                    hosts,
                    dataset,
                    shardsMap.values()
            ));
            LOGGER.info("Assigned all shards for " + dataset);
            return new Result(datasetDir, shardsMap.values());
        }
    }

    static class Result {
        private final RemoteCachingPath datasetDir;
        private final Collection<ShardDir> shards;

        Result(final RemoteCachingPath datasetDir, final Collection<ShardDir> shards) {
            this.datasetDir = datasetDir;
            this.shards = shards;
        }

        RemoteCachingPath getDatasetDir() {
            return datasetDir;
        }

        Collection<ShardDir> getShards() {
            return shards;
        }
    }

    static class Builder {
        private final HostsReloader hostsReloader;
        private final ShardAssigner shardAssigner;
        private final ShardAssignmentInfoDao assignmentInfoDao;
        private RemoteCachingPath datasetDir;
        private ShardFilter shardFilter;

        public Builder(final HostsReloader hostsReloader, final ShardAssigner shardAssigner, final ShardAssignmentInfoDao assignmentInfoDao) {
            this.hostsReloader = hostsReloader;
            this.shardAssigner = shardAssigner;
            this.assignmentInfoDao = assignmentInfoDao;
        }

        Builder setDatasetDir(final RemoteCachingPath datasetDir) {
            this.datasetDir = datasetDir;
            return this;
        }

        Builder setShardFilter(final ShardFilter shardFilter) {
            this.shardFilter = shardFilter;
            return this;
        }

        ShardScanWork build() {
            return new ShardScanWork(datasetDir, shardFilter,
                    hostsReloader, shardAssigner, assignmentInfoDao);
        }
    }
}
