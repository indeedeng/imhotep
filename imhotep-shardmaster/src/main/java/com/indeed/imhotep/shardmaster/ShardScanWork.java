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

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public Result call() {
        if (!hostsReloader.isLoadedDataSuccessfullyRecently()) {
            LOGGER.warn("Have not loaded hosts recently, will not try to reassign shards for " + datasetDir);
            return new Result(datasetDir, Collections.<ShardDir>emptyList());
        } else {
            final List<Host> hosts = hostsReloader.getHosts();
            final long upHosts = hosts.stream().filter(Objects::nonNull).count();

            final String dataset = datasetDir.getFileName().toString();

            final Map<String, ShardDir> shardsMap = new HashMap<>();
            for (final ShardDir shardDir : new ShardScanner(datasetDir, shardFilter)) {
                final ShardDir existing = shardsMap.get(shardDir.getId());
                if ((existing == null) || (existing.getVersion() < shardDir.getVersion())) {
                    shardsMap.put(shardDir.getId(), shardDir);
                }
            }
            LOGGER.info("Assigning " + shardsMap.size() + " shards in " + dataset + " for " + upHosts + " hosts");

            final Iterable<ShardAssignmentInfo> assignments = shardAssigner.assign(
                    hosts,
                    dataset,
                    shardsMap.values()
            );

            // Debug
//            List<ShardAssignmentInfo> assignmentInfoList = Lists.newArrayList(assignments);
//            testAssignments(assignmentInfoList, hosts, dataset);

            assignmentInfoDao.updateAssignments(dataset, DateTime.now(), assignments);
            LOGGER.info("Assigned all " + shardsMap.size() + " shards for " + dataset);
            return new Result(datasetDir, shardsMap.values());
        }
    }

    /**
     * Debug method for detecting uneven distribution of shards by a shard assigner
     */
    private void testAssignments(List<ShardAssignmentInfo> assignmentInfoList, List<Host> hosts, String dataset) {
        if(assignmentInfoList.size() == 0) return;
        final Multimap<Host, String> hostToShardCount = ArrayListMultimap.create();
        for (final ShardAssignmentInfo assignment : assignmentInfoList) {
            hostToShardCount.put(assignment.getAssignedNode(), assignment.getShardPath());
        }

        List<Integer> shardCountPerHost = Lists.newArrayList();
        for (final Map.Entry<Host, Collection<String>> entry : hostToShardCount.asMap().entrySet()) {
            shardCountPerHost.add(entry.getValue().size());
        }
        Collections.sort(shardCountPerHost);
        Collections.reverse(shardCountPerHost);
        System.out.println(dataset + ": (" + shardCountPerHost.get(0) + "," + shardCountPerHost.get(shardCountPerHost.size()-1) +
                ") - " + Joiner.on(',').join(shardCountPerHost));
        int numShards = assignmentInfoList.size();

        final double averageShardsPerHost = ((double) numShards) / hosts.size();
        final Integer maxShardCount = shardCountPerHost.get(0);
        double shardDiviation = Math.abs((averageShardsPerHost - maxShardCount));
        if(averageShardsPerHost > 10 &&  shardDiviation > 10 && shardDiviation / averageShardsPerHost > 0.05) {
            LOGGER.error("Uneven shard distribution detected. Average shards per host: " +
                    (int)averageShardsPerHost + ", shards at unbalanced host: " + maxShardCount);
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

        Builder(final HostsReloader hostsReloader, final ShardAssigner shardAssigner, final ShardAssignmentInfoDao assignmentInfoDao) {
            this.hostsReloader = hostsReloader;
            this.shardAssigner = shardAssigner;
            this.assignmentInfoDao = assignmentInfoDao;
        }

        Builder copy() {
            return new Builder(
                    hostsReloader,
                    shardAssigner,
                    assignmentInfoDao
            );
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
