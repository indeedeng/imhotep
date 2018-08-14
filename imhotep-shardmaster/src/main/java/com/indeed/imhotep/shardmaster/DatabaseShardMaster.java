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
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private final ShardAssigner assigner;
    private final ShardData shardData;
    private final HostsReloader reloader;

    public DatabaseShardMaster(final ShardAssigner assigner, final ShardData shardData, final HostsReloader reloader) {
        this.assigner = assigner;
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
            final List<String> conflictFields = shardData.getFields(dataset, ShardData.FieldType.CONFLICT);
            strFields.addAll(conflictFields);
            intFields.addAll(conflictFields);
            toReturn.add(new DatasetInfo(dataset, intFields, strFields));
        }
        return toReturn;
    }

    @Override
    public List<Shard> getShardsInTime(String dataset, long start, long end) {
        final Collection<ShardInfo> info = shardData.getShardsInTime(dataset, start, end);
        final List<Shard> shards = new ArrayList<>();
        final Iterable<Shard> assignment = assigner.assign(reloader.getHosts(), dataset, info);
        for(final Shard shardAndHost: assignment) {
            // TODO: move the .sqar to wherever we determine extensions
            final Shard shard = new Shard(shardAndHost.shardId, shardAndHost.numDocs, shardAndHost.version, shardAndHost.getServer(), ".sqar");
            shards.add(shard);
        }
        return shards;
    }

    @Override
    public Map<String, Collection<ShardInfo>> getShardList() {
        final Map<String, Collection<ShardInfo>> toReturn = new HashMap<>();
        final Collection<String> datasets = shardData.getDatasets();
        for(final String dataset: datasets) {
            final Collection<ShardInfo> shardsForDataset = shardData.getShardsForDataset(dataset);
            toReturn.put(dataset, shardsForDataset);
        }
        return toReturn;
    }
}
