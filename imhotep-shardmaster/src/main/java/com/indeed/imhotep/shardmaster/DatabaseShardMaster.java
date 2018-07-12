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

import com.indeed.imhotep.*;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.protobuf.AssignedShard;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.log4j.Logger;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private final ShardAssignmentInfoDao assignmentInfoDao;
    private final AtomicBoolean initializationComplete;
    private final ShardData shardData;
    private static final Logger LOGGER = Logger.getLogger(DatabaseShardMaster.class);

    public DatabaseShardMaster(final ShardAssignmentInfoDao assignmentInfoDao, final AtomicBoolean initializationComplete, final ShardData shardData) {
        this.assignmentInfoDao = assignmentInfoDao;
        this.initializationComplete = initializationComplete;
        this.shardData = shardData;
    }

    @Override
    public List<DatasetInfo> getDatasetMetadata() {
        final List<DatasetInfo> toReturn = new ArrayList<>();
        final Collection<String> datasets = shardData.getDatasets();
        for(final String dataset: datasets) {
            final List<String> strFields = shardData.getFields(dataset, ShardData.FieldType.STRING);
            final List<String> intFields = shardData.getFields(dataset, ShardData.FieldType.INT);
            final Collection<ShardDir> shards = shardData.getShardsForDataset(dataset);
            final List<ShardInfo> shardInfos = shards.stream().map(shardDir ->
                new ShardInfo(shardDir.getId(), shardData.getNumDocs(shardDir.getIndexDir().toString()), shardDir.getVersion())).collect(Collectors.toList());
            final long latestVersion = shardInfos.stream().map(shardInfo -> shardInfo.version).reduce(Long.MIN_VALUE, Long::max);
            toReturn.add(new DatasetInfo(dataset, shardInfos, intFields, strFields, latestVersion));
        }
        return toReturn;
    }

    @Override
    public List<Shard> getShardsInTime(String dataset, long start, long end) {
        final Collection<String> info = shardData.getShardsInTime(dataset, start, end);
        final List<Shard> shards = new ArrayList<>();
        for(final String path: info) {
            final ShardDir temp = new ShardDir(Paths.get(path));
            final List<Host> hosts = assignmentInfoDao.getAssignments(path);
            final Shard shard = new ShardWithPathAndDataset(new ShardDir(Paths.get(path)), shardData.getNumDocs(path));
            if(hosts == null) {
                LOGGER.error("Got no hosts. The path is: "+path);
                continue;
            }
            shard.servers.addAll(hosts);
            shards.add(shard);
        }
        return shards;
    }

    @Override
    public List<AssignedShard> getAssignments(final Host node) {
        if (!initializationComplete.get()) {
            throw new IllegalStateException("Initialization is not yet complete, please wait");
        }

        return StreamSupport.stream(assignmentInfoDao.getAssignments(node).spliterator(), true).map(shard -> AssignedShard.newBuilder()
                .setDataset(shard.getDataset())
                .setShardPath(shard.getShardPath())
                .build()).collect(Collectors.toList());
    }

    @Override
    public List<ShardWithPathAndDataset> getShardList() {
        final List<ShardWithPathAndDataset> toReturn = new ArrayList<>();
        final Collection<String> datasets = shardData.getDatasets();
        for(final String dataset: datasets) {
            final Collection<ShardDir> shardsForDataset = shardData.getShardsForDataset(dataset);
            for(ShardDir shardDir: shardsForDataset) {
                ShardWithPathAndDataset shard = new ShardWithPathAndDataset(shardDir, shardData.getNumDocs(shardDir.getIndexDir().toString()));
                toReturn.add(shard);
            }
        }
        return toReturn;
    }
}
