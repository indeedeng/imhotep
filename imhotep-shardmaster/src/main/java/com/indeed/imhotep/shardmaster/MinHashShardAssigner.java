package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import com.indeed.util.core.Pair;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * @author kenh
 */
@ThreadSafe
class MinHashShardAssigner implements ShardAssigner {
    private final int replicationFactor;

    private static final ThreadLocal<HashFunction> HASH_FUNCTION = new ThreadLocal<HashFunction>() {
        @Override
        protected HashFunction initialValue() {
            return Hashing.goodFastHash(32);
        }
    };

    MinHashShardAssigner(final int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    private long getMinHash(final String dataset, final ShardDir shard, final Host host) {
        return HASH_FUNCTION.get().newHasher()
                .putInt(dataset.hashCode())
                .putInt(shard.getId().hashCode())
                .putInt(host.getHostname().hashCode())
                .putInt(host.getPort())
                .hash().asInt();
    }

    @Override
    public Iterable<ShardAssignmentInfo> assign(final List<Host> hosts, final String dataset, final Iterable<ShardDir> shards) {
        int maxPerHostname = 0;
        for (final Collection<Host> ofSameHostName : FluentIterable.from(hosts).index(Host.GET_HOSTNAME).asMap().values()) {
            maxPerHostname = Math.max(maxPerHostname, ofSameHostName.size());
        }
        final int queueCapacity = replicationFactor * maxPerHostname;
        final Pair.FullPairComparator comparator = new Pair.FullPairComparator();

        return FluentIterable.from(shards).transformAndConcat(new Function<ShardDir, Iterable<ShardAssignmentInfo>>() {
            @Override
            public Iterable<ShardAssignmentInfo> apply(final ShardDir shard) {
                final PriorityQueue<Pair<Long, Host>> sortedHosts = new PriorityQueue<>(queueCapacity,
                        Ordering.from(comparator).reverse());

                for (final Host host : hosts) {
                    final long hash = getMinHash(dataset, shard, host);
                    final Pair<Long, Host> entry = Pair.of(hash, host);
                    if (sortedHosts.size() < queueCapacity) {
                        sortedHosts.add(entry);
                    } else {
                        final Pair<Long, Host> largest = sortedHosts.peek();
                        if (comparator.compare(entry, largest) < 0) {
                            sortedHosts.remove();
                            sortedHosts.add(entry);
                        }
                    }
                }

                final List<Host> sortedHostsList = new ArrayList<>(sortedHosts.size());
                while (!sortedHosts.isEmpty()) {
                    sortedHostsList.add(sortedHosts.poll().getSecond());
                }

                final Set<String> hostnames = Sets.newHashSet();
                final List<Host> chosen = new ArrayList<>(replicationFactor);
                for (int i = sortedHostsList.size() - 1; i >= 0; i--) {
                    final Host sortedHost = sortedHostsList.get(i);
                    if (!hostnames.contains(sortedHost.getHostname())) {
                        hostnames.add(sortedHost.getHostname());
                        chosen.add(sortedHost);
                        if (chosen.size() >= replicationFactor) {
                            break;
                        }
                    }
                }

                return FluentIterable.from(chosen).transform(new Function<Host, ShardAssignmentInfo>() {
                    @Override
                    public ShardAssignmentInfo apply(final Host chosenHost) {
                        return new ShardAssignmentInfo(
                                dataset,
                                shard.getIndexDir().toUri().toString(),
                                chosenHost
                        );
                    }
                });
            }
        });
    }
}
