package com.indeed.imhotep.shardmanager;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.archive.ArchiveUtils;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;
import com.indeed.util.core.Pair;

import javax.annotation.concurrent.ThreadSafe;
import java.security.MessageDigest;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author kenh
 */
@ThreadSafe
class MinHashShardAssigner implements ShardAssigner {
    private final int replicationFactor;
    private static final ThreadLocal<MessageDigest> MD5_DIGEST = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            return ArchiveUtils.getMD5Digest();
        }
    };

    MinHashShardAssigner(final int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    private long getMinHash(final ShardDir shard, final Host host) {
        final MessageDigest messageDigest = MD5_DIGEST.get();
        messageDigest.reset();
        messageDigest.update(shard.getId().getBytes(Charsets.UTF_8));
        return Longs.fromByteArray(messageDigest.digest(host.getHostname().getBytes(Charsets.UTF_8)));
    }

    @Override
    public Iterable<ShardAssignmentInfo> assign(final List<Host> hosts, final String dataset, final Iterable<ShardDir> shards) {
        return FluentIterable.from(shards).transformAndConcat(new Function<ShardDir, Iterable<ShardAssignmentInfo>>() {
            @Override
            public Iterable<ShardAssignmentInfo> apply(final ShardDir shard) {
                final PriorityQueue<Pair<Long, Host>> candidates = new PriorityQueue<>(replicationFactor,
                        Ordering.from(new Pair.HalfPairComparator()).reverse());
                for (final Host host : hosts) {
                    final long hash = getMinHash(shard, host);
                    if (candidates.size() < replicationFactor) {
                        candidates.add(Pair.of(hash, host));
                    } else {
                        final Pair<Long, Host> largest = candidates.peek();
                        if (hash <= largest.getFirst()) {
                            candidates.remove();
                            candidates.add(Pair.of(hash, host));
                        }
                    }
                }

                return FluentIterable.from(candidates).transform(new Function<Pair<Long, Host>, ShardAssignmentInfo>() {
                    @Override
                    public ShardAssignmentInfo apply(final Pair<Long, Host> chosenHost) {
                        return new ShardAssignmentInfo(
                                dataset,
                                shard.getId(),
                                shard.getIndexDir().toUri().toString(),
                                chosenHost.getSecond().getHostname()
                        );
                    }
                });
            }
        });
    }
}
