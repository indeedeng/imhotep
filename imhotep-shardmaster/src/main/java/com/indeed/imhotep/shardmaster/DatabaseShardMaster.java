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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.HostsReloader;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author kenh
 */

public class DatabaseShardMaster implements ShardMaster {
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(Integer.MAX_VALUE);

    private final ShardAssigner assigner;
    private final ShardData shardData;
    private final HostsReloader reloader;
    private final ShardRefresher refresher;
    // this field is for imhotep performance test, see IMTEPD-516
    private final Map<String, Integer> datasetHostLimitMap;

    public DatabaseShardMaster(
            final ShardAssigner assigner,
            final ShardData shardData,
            final HostsReloader reloader,
            final ShardRefresher refresher,
            final Map<String, Integer> datasetHostLimitMap) {
        this.assigner = assigner;
        this.shardData = shardData;
        this.reloader = reloader;
        this.refresher = refresher;
        this.datasetHostLimitMap = datasetHostLimitMap;
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
    public List<Shard> getShardsInTime(final String dataset, final long start, final long end) {
        final Collection<ShardInfo> info = shardData.getShardsInTime(dataset, start, end);
        List<Host> hosts = reloader.getHosts();
        if (datasetHostLimitMap.containsKey(dataset)) {
            final int hostLimit = datasetHostLimitMap.get(dataset);
            // the order of hosts in list is determinate.
            // (1) for ZkHostsReloader, it sorts the host list when returning them
            // (2) for DummyHostsReloader, the host list never changes
            hosts = chooseHosts(dataset, hosts, hostLimit);
        }
        return ImmutableList.copyOf(assigner.assign(hosts, dataset, info));
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

    @Override
    public void refreshFieldsForDataset(final String dataset) throws IOException {
        refresher.refreshFieldsForDatasetInSQL(dataset);
    }

    private List<Host> chooseHosts(final String dataset, final List<Host> hosts, final int hostLimit) {
        final int datasetHashCode = Objects.hashCode(dataset);
        return IntStream.range(0, hosts.size())
                .boxed()
                .sorted(Comparator.comparingInt(hostIndex ->
                        HASH_FUNCTION.newHasher()
                                .putInt(datasetHashCode)
                                .putInt(hostIndex)
                                .hash()
                                .asInt()))
                .limit(hostLimit)
                .map(hostIndex -> hosts.get(hostIndex))
                .collect(Collectors.toList());
    }
}
