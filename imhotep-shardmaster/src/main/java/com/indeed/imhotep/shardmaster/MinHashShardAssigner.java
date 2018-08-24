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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.Host;
import com.indeed.util.core.Pair;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A shard assigner that uses a consistent hashing algorithm.
 * It results in non-perfect distribution for queries unless either:
 * 1) replication factor is at least 2 which lets the client to balance by picking less loaded servers
 * 2) #shards in query is many times higher than #servers
 *
 * The advantage of this assigner is that it allows only minimal number of shards to move during rebalancing
 * when expanding the cluster.
 * @author kenh
 */
@ThreadSafe
class MinHashShardAssigner implements ShardAssigner {
    private final int replicationFactor;

    private static final ThreadLocal<HashFunction> HASH_FUNCTION = new ThreadLocal<HashFunction>() {
        @Override
        protected HashFunction initialValue() {
            return Hashing.murmur3_32((int)1515546902721L);
        }
    };

    MinHashShardAssigner(final int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    private static long getMinHash(final String dataset, final ShardInfo shard, final Host host) {
        return HASH_FUNCTION.get().newHasher()
                .putInt(dataset.hashCode())
                .putInt(shard.shardId.hashCode())
                .putInt(host.getHostname().hashCode())
                .putInt(host.getPort())
                .hash().asInt();
    }

    @Override
    public Iterable<Shard> assign(final List<Host> hosts, final String dataset, final Iterable<ShardInfo> shards) {
        return assign(hosts, dataset, shards, replicationFactor);
    }

    public static Iterable<Shard> assign(final List<Host> hosts, final String dataset, final Iterable<ShardInfo> shards, final int replicationFactorUsed) {
        int maxPerHostname = 0;
        for (final Collection<Host> ofSameHostName : FluentIterable.from(hosts).filter(Objects::nonNull).index(Host.GET_HOSTNAME).asMap().values()) {
            maxPerHostname = Math.max(maxPerHostname, ofSameHostName.size());
        }
        final int queueCapacity = replicationFactorUsed * maxPerHostname;
        final Pair.FullPairComparator comparator = new Pair.FullPairComparator();

        return FluentIterable.from(shards).transformAndConcat(new Function<ShardInfo, Iterable<Shard>>() {
            @Override
            public Iterable<Shard> apply(final ShardInfo shard) {
                final PriorityQueue<Pair<Long, Host>> sortedHosts = new PriorityQueue<>(queueCapacity,
                        Ordering.from(comparator).reverse());

                for (final Host host : hosts) {
                    if(host == null) {
                        continue;   // this host is disabled
                    }
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
                final List<Host> chosen = new ArrayList<>(replicationFactorUsed);
                for (int i = sortedHostsList.size() - 1; i >= 0; i--) {
                    final Host sortedHost = sortedHostsList.get(i);
                    if (!hostnames.contains(sortedHost.getHostname())) {
                        hostnames.add(sortedHost.getHostname());
                        chosen.add(sortedHost);
                        if (chosen.size() >= replicationFactorUsed) {
                            break;
                        }
                    }
                }

                return FluentIterable.from(chosen).transform(new Function<Host, Shard>() {
                    @Override
                    public Shard apply(final Host chosenHost) {
                        return new Shard(
                                shard.shardId,
                                shard.numDocs,
                                shard.version,
                                chosenHost
                        );
                    }
                });
            }
        });
    }
}
