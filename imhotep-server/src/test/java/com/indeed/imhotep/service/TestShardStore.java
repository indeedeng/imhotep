/*
 * Copyright (C) 2016 Indeed Inc.
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
package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.indeed.imhotep.ShardInfo;
import com.indeed.lsmtree.core.Store;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class TestShardStore {

    private static final Random rng = new Random(0xdeadbeef);

    @Rule
    public final TemporaryFolder rootTestDir = new TemporaryFolder();

    private Path storeDir = null;

    @Before
    public void setUp() {
        storeDir = rootTestDir.newFolder("store").toPath();
    }

    @Test public void testShardStore() {
        try {
            final SortedMap<ShardStore.Key, ShardStore.Value> before = generateRandomEntries(64, 512);

            /* Create the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               for (final Map.Entry<ShardStore.Key, ShardStore.Value> entry: before.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }

               final Map<ShardStore.Key, ShardStore.Value> after =
                   new TreeMap<>();
               final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                   store.iterator();
               while (it.hasNext()) {
                   final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                   after.put(entry.getKey(), entry.getValue());
               }
               assertEquals(after, before);
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(10000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               final Map<ShardStore.Key, ShardStore.Value> after =
                   new TreeMap<>();
               final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                   store.iterator();
               while (it.hasNext()) {
                   final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                   after.put(entry.getKey(), entry.getValue());
               }
               assertEquals(after, before);
            }
        }
        catch (final Exception ex) {
            Throwables.propagate(ex);
        }
    }

    @Test public void testShardInfoList() {
        try {
            final SortedMap<ShardStore.Key, ShardStore.Value> entries = generateRandomEntries(64, 512);

            final List<ShardInfo> expected = new ObjectArrayList<>(entries.size());
            for (final Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                final ShardStore.Key   key   = entry.getKey();
                final ShardStore.Value value = entry.getValue();
                final ShardInfo shardInfo =
                    new ShardInfo(key.getDataset(), key.getShardId(),
                                  new ObjectArrayList<String>(0),
                                  value.getNumDocs(),
                                  value.getVersion());
                expected.add(shardInfo);
            }
            Collections.sort(expected, ShardInfoList.comparator);

            try (ShardStore store = new ShardStore(storeDir)) {
               for (final Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(10000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               final ShardInfoList actual = new ShardInfoList(store);
               assertEquals(expected, actual);
         }
        }
        catch (final Exception ex) {
            Throwables.propagate(ex);
        }
    }

    @Test public void testDatasetInfoList() {
        try {
            final SortedMap<ShardStore.Key, ShardStore.Value> entries = generateRandomEntries(64, 512);

            try (ShardStore store = new ShardStore(storeDir)) {
               for (final Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(10000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
                final DatasetInfoList actual = new DatasetInfoList(store);
                /* TODO(johnf): test the content in some meaningful way (without
                 just recreating the DatasetInfoList logic in this test. */
         }
        }
        catch (final Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private ObjectArrayList<String> newRandomFieldList() {
        final int numFields = rng.nextInt(128);
        final ObjectArrayList<String> result = new ObjectArrayList<>(numFields);
        for (int count = 0; count < numFields; ++count) {
            final String field = RandomStringUtils.randomAlphanumeric(8 + rng.nextInt(32));
            result.add(field);
        }
        return result;
    }

    private SortedMap<ShardStore.Key, ShardStore.Value> generateRandomEntries(final int numDatasets,
                                                                              final int maxShardsPerDataset) {
        final SortedMap<ShardStore.Key, ShardStore.Value> result = new TreeMap<>();
        for (int cDataset = 0; cDataset < numDatasets; ++cDataset) {
            final String dataset = RandomStringUtils.randomAlphanumeric(16);
            final int numShards = 1 + rng.nextInt(maxShardsPerDataset);
            for (int cShard = 0; cShard < numShards; ++cShard) {
                final String shardId = RandomStringUtils.randomAlphanumeric(64);
                final int numDocs = 1 + rng.nextInt(256);
                final long version = 1 + rng.nextInt(3);
                final ObjectArrayList<String> intFields = newRandomFieldList();
                final ObjectArrayList<String> strFields = newRandomFieldList();
                final String shardDir =
                    "/var/shards/" + shardId + Long.valueOf(version).toString();
                final ShardStore.Key key = new ShardStore.Key(dataset, shardId);
                final ShardStore.Value value =
                    new ShardStore.Value(shardDir, numDocs, version, intFields, strFields);
                result.put(key, value);
            }
        }
        return result;
    }
}
