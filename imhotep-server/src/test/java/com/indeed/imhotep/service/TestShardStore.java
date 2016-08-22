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

import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.lsmtree.core.Store;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestShardStore {

    private static final Random rng = new Random(0xdeadbeef);

    @Rule
    public final TemporaryFolder rootTestDir = new TemporaryFolder();

    private Path storeDir   = null;

    @Before
    public void setUp() throws Exception {
        storeDir   = rootTestDir.newFolder("store").toPath();
    }

    @Test public void testShardStore() {
        try {
            RandomEntries before = new RandomEntries(64, 512);

            /* Create the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: before.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }

               TreeMap<ShardStore.Key, ShardStore.Value> after =
                   new TreeMap<ShardStore.Key, ShardStore.Value>();
               Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                   store.iterator();
               while (it.hasNext()) {
                   Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                   after.put(entry.getKey(), entry.getValue());
               }
               assertEquals(before, after);
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(1000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               TreeMap<ShardStore.Key, ShardStore.Value> after =
                   new TreeMap<ShardStore.Key, ShardStore.Value>();
               Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                   store.iterator();
               while (it.hasNext()) {
                   Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                   after.put(entry.getKey(), entry.getValue());
               }
               assertEquals(before, after);
            }
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test public void testShardInfoList() {
        try {
            RandomEntries entries = new RandomEntries(64, 512);

            List<ShardInfo> expected = new ObjectArrayList<ShardInfo>(entries.size());
            for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
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
               for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(1000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
               ShardInfoList actual = new ShardInfoList(store);
               assertEquals(expected, actual);
         }
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test public void testDatasetInfoList() {
        try {
            RandomEntries entries = new RandomEntries(64, 512);

            try (ShardStore store = new ShardStore(storeDir)) {
               for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(1000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(storeDir)) {
                DatasetInfoList actual = new DatasetInfoList(store);
                /* TODO(johnf): test the content in some meaningful way (without
                 just recreating the DatasetInfoList logic in this test. */
         }
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    private ObjectArrayList<String> newRandomFieldList() {
        final int numFields = rng.nextInt(128);
        final ObjectArrayList<String> result = new ObjectArrayList<String>(numFields);
        for (int count = 0; count < numFields; ++count) {
            final String field = RandomStringUtils.randomAlphanumeric(8 + rng.nextInt(32));
            result.add(field);
        }
        return result;
    }

    private class RandomEntries extends TreeMap<ShardStore.Key, ShardStore.Value> {
        RandomEntries(int numDatasets, int maxShardsPerDataset) {
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
                    put(key, value);
                }
            }
        }
    }
}
