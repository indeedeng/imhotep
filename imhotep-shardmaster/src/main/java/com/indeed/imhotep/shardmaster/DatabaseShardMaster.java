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
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.log4j.Logger;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private final ShardAssigner assigner;
    private final AtomicBoolean initializationComplete;
    private final ShardData shardData;
    private final HostsReloader reloader;
    private static final Logger LOGGER = Logger.getLogger(DatabaseShardMaster.class);

    public DatabaseShardMaster(final ShardAssigner assigner, final AtomicBoolean initializationComplete, final ShardData shardData, final HostsReloader reloader) {
        this.assigner = assigner;
        this.initializationComplete = initializationComplete;
        this.shardData = shardData;
        this.reloader = reloader;
    }

    @Override
    public List<DatasetInfo> getDatasetMetadata() {
        final List<DatasetInfo> toReturn = new ArrayList<>();
        final Collection<String> datasets = shardData.getDatasets();
        for(final String dataset: datasets) {
            final List<String> strFields = shardData.getFields(dataset, ShardData.FieldType.STRING);
            final List<String> intFields = shardData.getFields(dataset, ShardData.FieldType.INT);
            toReturn.add(new DatasetInfo(dataset, intFields, strFields));
        }
        return toReturn;
    }

    @Override
    public List<Shard> getShardsInTime(String dataset, long start, long end) {
        final Collection<String> info = shardData.getShardsInTime(dataset, start, end);
        final List<Shard> shards = new ArrayList<>();
        final Iterable<ShardAssignmentInfo> assignment = assigner.assign(reloader.getHosts(), dataset, info.stream().map(str -> new ShardDir(Paths.get(str))).collect(Collectors.toList()));
        for(final ShardAssignmentInfo shardAndHost: assignment) {
            ShardDir dir = new ShardDir(Paths.get(shardAndHost.getShardPath()));
            // TODO: move the .sqar to wherever we determine extensions
            final Shard shard = new Shard(dir.getId(), shardData.getNumDocs(shardAndHost.getShardPath()), dir.getVersion(), shardAndHost.getAssignedNode(), ".sqar");
            shards.add(shard);
        }
        return shards;
    }

    @Override
    public Map<String, List<ShardInfo>> getShardList() {
        final Map<String, List<ShardInfo>> toReturn = new HashMap<>();
        final Collection<String> datasets = shardData.getDatasets();
        for(final String dataset: datasets) {
            final Collection<ShardDir> shardsForDataset = shardData.getShardsForDataset(dataset);
            List<ShardInfo> shards = new ArrayList<>();
            for(ShardDir shardDir: shardsForDataset) {
                ShardInfo shard = new ShardInfo(shardDir.getId(), shardData.getNumDocs(shardDir.getIndexDir().toString()), shardDir.getVersion());
                shards.add(shard);
            }
            toReturn.put(dataset, shards);
        }
        return toReturn;
    }
}
