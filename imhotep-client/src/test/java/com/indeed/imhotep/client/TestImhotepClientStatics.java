package com.indeed.imhotep.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author vladimir
 */

public class TestImhotepClientStatics {

    @Test
    public void testGetDatasetToShardList() throws IOException {
        final String dataset = "sponsored";
        final Collection<List<DatasetInfo>> hostsDatasets = Lists.newArrayList();
        Set<String> expectedIntFields;
        Set<String> expectedStringFields;

        hostsDatasets.clear();
        addHostShards(hostsDatasets, dataset, new long[]{20150101000000L}, ImmutableSet.of("int1"), ImmutableSet.of("str1"));
        addHostShards(hostsDatasets, dataset, new long[]{20150102000000L}, ImmutableSet.of("int1", "int2"), ImmutableSet.of("str1", "str2"));    // newest
        addHostShards(hostsDatasets, dataset, new long[]{20150101000000L, 20150102000000L, 20150103000000L}, ImmutableSet.of("int1", "int2"), ImmutableSet.of("str1", "str2"));
        expectedIntFields = ImmutableSet.of("int1", "int2");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(hostsDatasets, expectedIntFields, expectedStringFields);

        hostsDatasets.clear();
        addHostShards(hostsDatasets, dataset, new long[]{20150101000000L}, ImmutableSet.of("int1", "int2", "conflict"), ImmutableSet.of("str1", "str2", "conflict"));
        addHostShards(hostsDatasets, dataset, new long[]{20150102000000L}, ImmutableSet.of("int1", "int2", "conflict"), ImmutableSet.of("str1", "str2", "conflict"));
        addHostShards(hostsDatasets, dataset, new long[]{Long.MAX_VALUE, 20150103000000L}, ImmutableSet.of("int1", "int2", "conflict"), ImmutableSet.of("str1", "str2"));     // newest
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(hostsDatasets, expectedIntFields, expectedStringFields);

        hostsDatasets.clear();
        addHostShards(hostsDatasets, dataset, new long[]{20150103000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2"));     // newest
        addHostShards(hostsDatasets, dataset, new long[]{20150101000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2", "conflict"));
        addHostShards(hostsDatasets, dataset, new long[]{20150102000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2", "conflict"));
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2");
        checkExpectedFields(hostsDatasets, expectedIntFields, expectedStringFields);

        hostsDatasets.clear();
        addHostShards(hostsDatasets, dataset, new long[]{20150103000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2", "conflict"));     // newest
        addHostShards(hostsDatasets, dataset, new long[]{20150101000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2", "conflict"));
        addHostShards(hostsDatasets, dataset, new long[]{20150102000000L}, ImmutableSet.of("conflict", "int1", "int2"), ImmutableSet.of("str1", "str2", "conflict"));
        expectedIntFields = ImmutableSet.of("int1", "int2", "conflict");
        expectedStringFields = ImmutableSet.of("str1", "str2", "conflict");
        checkExpectedFields(hostsDatasets, expectedIntFields, expectedStringFields);
    }

    private void checkExpectedFields(
            final Collection<List<DatasetInfo>> hostsDatasets,
            final Set<String> expectedIntFields,
            final Set<String> expectedStringFields) throws IOException {
        final Map<String, DatasetInfo> datasetInfos = ImhotepClient.getDatasetToDatasetInfo(hostsDatasets, false);
        assertEquals(1, datasetInfos.size());

        final DatasetInfo datasetInfo = datasetInfos.values().iterator().next();
        assertEquals(expectedIntFields, datasetInfo.getIntFields());
        assertEquals(expectedStringFields, datasetInfo.getStringFields());
    }

    private void addHostShards(
            final Collection<List<DatasetInfo>> hostsDatasets,
            final String dataset,
            final long[] versions,
            final ImmutableSet<String> intFields,
            final ImmutableSet<String> stringFields) throws IOException {
        final List<ShardInfo> shardInfos = Lists.newArrayList();
        for(final long version: versions) {
            final String shardId = "index" + Long.toString(version).substring(0, 8);
            final ShardInfo shardInfo = new ShardInfo(shardId, 0, version);
            shardInfos.add(shardInfo);
        }
        long latestVersion = 0;
        for (long version: versions) {
            latestVersion = Math.max(latestVersion, version);
        }
        final DatasetInfo datasetInfo = new DatasetInfo(dataset, shardInfos, intFields, stringFields, latestVersion);
        hostsDatasets.add(Lists.newArrayList(datasetInfo));
    }
}
