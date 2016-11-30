package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

        final List<Host> hosts = FluentIterable.from(getListWithPrefix("HOST", 1, 10))
                .transformAndConcat(new Function<String, List<Host>>() {
                    @Override
                    public List<Host> apply(final String host) {
                        return Arrays.asList(
                                new Host(host, 8080),
                                new Host(host, 8081)
                        );
                    }
                }).toList();
        final int numHosts = hosts.size();

        final Multimap<Host, String> hostToShardCount = ArrayListMultimap.create();
        final Set<String> assignedShards = Sets.newHashSet();

        final int replicationFactor = 3;
        final MinHashShardAssigner assigner = new MinHashShardAssigner(replicationFactor);
        for (final ShardAssignmentInfo assignment : assigner.assign(hosts, "DATASET", shards)) {
            Assert.assertEquals("DATASET", assignment.getDataset());
            assignedShards.add(assignment.getShardPath());
            hostToShardCount.put(assignment.getAssignedNode(), assignment.getShardPath());
        }

        Assert.assertEquals(shards.size(), assignedShards.size());

        final double shardsPerHost = ((double) numShards * replicationFactor) / numHosts;
        for (final Map.Entry<Host, Collection<String>> entry : hostToShardCount.asMap().entrySet()) {
            Assert.assertEquals(entry.getKey() + ":" + entry.getValue().size(), 0, (shardsPerHost - entry.getValue().size()) / shardsPerHost, 0.05);
        }
    }

    @Test
    public void testReplicationWithMultiplePerHosts() {
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
                .transformAndConcat(new Function<String, List<Host>>() {
                    @Override
                    public List<Host> apply(final String host) {
                        return Arrays.asList(
                                new Host(host, 8080),
                                new Host(host, 8081)
                        );
                    }
                }).toList();

        final HashMultimap<String, Host> shardToHosts = HashMultimap.create();
        final Set<String> assignedShards = Sets.newHashSet();

        final int replicationFactor = 3;
        final MinHashShardAssigner assigner = new MinHashShardAssigner(replicationFactor);
        for (final ShardAssignmentInfo assignment : assigner.assign(hosts, "DATASET", shards)) {
            Assert.assertEquals("DATASET", assignment.getDataset());
            assignedShards.add(assignment.getShardPath());
            shardToHosts.put(assignment.getShardPath(), assignment.getAssignedNode());
        }

        Assert.assertEquals(shards.size(), assignedShards.size());

        for (final Map.Entry<String, Collection<Host>> entry : shardToHosts.asMap().entrySet()) {
            final List<Host> assignedHosts = new ArrayList<>(entry.getValue());
            Assert.assertEquals(replicationFactor, assignedHosts.size());
            // ensure same shard is not allocated to same hostname twice
            for (int i = 0; i < assignedHosts.size(); i++) {
                final String hostname = assignedHosts.get(i).getHostname();
                for (int j = i + 1; j < assignedHosts.size(); j++) {
                    Assert.assertFalse(hostname.equals(assignedHosts.get(j).getHostname()));
                }
            }
        }
    }

    @Test
    public void testConsistency() {
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