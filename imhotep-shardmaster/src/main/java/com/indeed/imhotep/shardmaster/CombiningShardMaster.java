package com.indeed.imhotep.shardmaster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombiningShardMaster implements ShardMaster {
    private final List<ShardMaster> shardMasters;

    public CombiningShardMaster(final List<ShardMaster> shardMasters) {
        this.shardMasters = shardMasters;
    }

    public CombiningShardMaster(final ShardMaster... shardMasters) {
        this(ImmutableList.copyOf(shardMasters));
    }

    @Override
    public List<DatasetInfo> getDatasetMetadata() throws IOException {
        final List<DatasetInfo> combinedResult = new ArrayList<>();
        for (final ShardMaster shardMaster : shardMasters) {
            combinedResult.addAll(shardMaster.getDatasetMetadata());
        }
        return combinedResult;
    }

    @Override
    public List<Shard> getShardsInTime(final String dataset, final long start, final long end) throws IOException {
        final List<Shard> combinedResult = new ArrayList<>();
        for (final ShardMaster shardMaster : shardMasters) {
            combinedResult.addAll(shardMaster.getShardsInTime(dataset, start, end));
        }
        return combinedResult;
    }

    @Override
    public Map<String, Collection<ShardInfo>> getShardList() throws IOException {
        final Map<String, Collection<ShardInfo>> combinedResult = new HashMap<>();
        for (final ShardMaster shardMaster : shardMasters) {
            for (final Map.Entry<String, Collection<ShardInfo>> entry : shardMaster.getShardList().entrySet()) {
                combinedResult.merge(entry.getKey(), entry.getValue(), (lhs, rhs) -> ImmutableList.copyOf(Iterables.concat(lhs, rhs)));
            }
        }
        return combinedResult;
    }

    @Override
    public void refreshFieldsForDataset(final String dataset) throws IOException {
        for (final ShardMaster shardMaster : shardMasters) {
            shardMaster.refreshFieldsForDataset(dataset);
        }
    }
}
