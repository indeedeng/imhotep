package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author kenh
 */

public class MinHashShardAssignerTest {

    private static List<String> getListWithPrefix(final String prefix, final int start, final int end) {
        final List<String> list = new ArrayList<>();
        for (int i = start; i < end; ++i) {
            list.add(prefix + i);
        }
        return list;
    }

    @Test
    public void testEvenDistribution() {
        final int numShards = 10000;
        final List<ShardDir> shards = FluentIterable.from(getListWithPrefix("SHARD", 1, numShards))
                .transform(new Function<String, ShardDir>() {
                    @Override
                    public ShardDir apply(final String shard) {
                        return new ShardDir(Paths.get(shard));
                    }
                }).toList();

        final int numHosts = 10;
        final List<Host> hosts = FluentIterable.from(getListWithPrefix("HOST", 1, numHosts))
                .transform(new Function<String, Host>() {
                    @Override
                    public Host apply(final String host) {
                        return new Host(host, 8080);
                    }
                }).toList();

        final Map<String, Integer> hostToShardCount = Maps.newHashMap();
        final Set<String> assignedShards = Sets.newHashSet();

        final int replicationFactor = 3;
        final MinHashShardAssigner assigner = new MinHashShardAssigner(replicationFactor);
        for (final ShardAssignmentInfo assignment : assigner.assign(hosts, "DATASET", shards)) {
            Assert.assertEquals("DATASET", assignment.getDataset());
            assignedShards.add(assignment.getShardId());
            final Integer count = hostToShardCount.get(assignment.getAssignedNode());
            if (count == null) {
                hostToShardCount.put(assignment.getAssignedNode(), 1);
            } else {
                hostToShardCount.put(assignment.getAssignedNode(), count + 1);
            }
        }

        Assert.assertEquals(shards.size(), assignedShards.size());

        final double shardsPerHost = ((double) numShards * replicationFactor) / numHosts;
        for (final Map.Entry<String, Integer> entry : hostToShardCount.entrySet()) {
            Assert.assertEquals(entry.toString(), 0, (shardsPerHost - entry.getValue()) / numShards, 1e-1);
        }
    }

    @Test
    public void testConsitency() {
        final ShardDir shard = new ShardDir(Paths.get("SHARD"));

        final int replicationFactor = 3;
        final MinHashShardAssigner assigner = new MinHashShardAssigner(replicationFactor);

        Set<ShardAssignmentInfo> prevChosen = Collections.emptySet();
        final List<Host> hosts = new ArrayList<>();
        final int numHosts = 1000;

        // incrementally add new nodes and ensure that only 1 node is evicted from the chosen set of candidates
        for (int i = 0; i < numHosts; i++) {
            hosts.add(new Host("HOST" + i, 8080));

            final Set<ShardAssignmentInfo> chosen = FluentIterable
                    .from(assigner.assign(hosts, "DATASET", Collections.singletonList(shard))).toSet();

            Assert.assertEquals(Math.min(hosts.size(), replicationFactor), chosen.size());

            final Sets.SetView<ShardAssignmentInfo> diff = Sets.difference(prevChosen, chosen);
            Assert.assertTrue(diff.size() <= 1);

            prevChosen = chosen;
        }
    }
}