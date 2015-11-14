package com.indeed.imhotep.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

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

    private void checkExpectedFields(Collection<List<DatasetInfo>> hostsDatasets, Set<String> expectedIntFields, Set<String> expectedStringFields) throws IOException {
        Map<String, DatasetInfo> datasetInfos = ImhotepClient.getDatasetToShardList(hostsDatasets);
        assertEquals(1, datasetInfos.size());

        DatasetInfo datasetInfo = datasetInfos.values().iterator().next();
        assertEquals(expectedIntFields, datasetInfo.getIntFields());
        assertEquals(expectedStringFields, datasetInfo.getStringFields());
    }

    private void addHostShards(Collection<List<DatasetInfo>> hostsDatasets, String dataset, long[] versions, ImmutableSet<String> intFields, ImmutableSet<String> stringFields) throws IOException {
        List<ShardInfo> shardInfos = Lists.newArrayList();
        for(long version: versions) {
            String shardId = "index" + Long.toString(version).substring(0, 8);
            ShardInfo shardInfo = new ShardInfo(dataset, shardId, Collections.<String>emptyList(), 0, version);
            shardInfos.add(shardInfo);
        }
        DatasetInfo datasetInfo = new DatasetInfo(dataset, shardInfos, intFields, stringFields, Collections.<String>emptyList());
        hostsDatasets.add(Lists.newArrayList(datasetInfo));
    }
}
