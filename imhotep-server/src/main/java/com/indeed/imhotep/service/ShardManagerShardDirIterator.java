package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.shardmanager.ShardManager;
import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * @author kenh
 */

public class ShardManagerShardDirIterator implements  ShardDirIterator {
    private final Supplier<ShardManager> shardManager;
    private final String node;

    public ShardManagerShardDirIterator(final Supplier<ShardManager> shardManager, final String node) {
        this.shardManager = shardManager;
        this.node = node;
    }

    @Override
    public Iterator<Pair<String, ShardDir>> iterator() {
        final Iterable<AssignedShard> assignments;
        try {
            assignments = shardManager.get().getAssignments(node);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shard assignment for " + node, e);
        }

        return FluentIterable.from(assignments).transform(new Function<AssignedShard, Pair<String, ShardDir>>() {
            @Override
            public Pair<String, ShardDir> apply(final AssignedShard shard) {
                return Pair.of(shard.getDataset(), new ShardDir(Paths.get(shard.getShardPath())));
            }
        }).iterator();
    }
}
