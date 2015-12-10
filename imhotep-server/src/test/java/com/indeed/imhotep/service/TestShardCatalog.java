package com.indeed.imhotep.service;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.util.io.Files;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class TestShardCatalog {

    private static final Random rng = new Random(0xdeadbeef);

    private String diDir = null;
    private String siDir = null;

    @Before public void setUp() throws Exception {
        diDir = Files.getTempDirectory("DatasetInfoStore.", ".delete.me");
        siDir = Files.getTempDirectory("ShardInfoStore.", ".delete.me");
    }

    @After public void tearDown() throws Exception {
        if (diDir != null) Files.delete(diDir);
        if (siDir != null) Files.delete(siDir);
    }


    @Test public void testShardInfoStore() {
        try {
            RandomShardInfoSet before = new RandomShardInfoSet(4096);
            /* Create the store and verify that it has everything in it. */
            try (ShardInfoStore store = new ShardInfoStore(new File(siDir))) {
                    for (ShardInfo shardInfo: before) {
                        store.put(shardInfo);
                    }

                    HashSet<ShardInfo> after = new HashSet<ShardInfo>();
                    Iterator<ShardInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }

                    assertEquals(before, after);
                }

            /* Reopen the store and verify that it has everything in it. */
            try (ShardInfoStore store = new ShardInfoStore(new File(siDir))) {
                    HashSet<ShardInfo> after = new HashSet<ShardInfo>();
                    Iterator<ShardInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }

                    assertEquals(before, after);
                }
        }
        catch (IOException ex) {
            fail(ex.toString());
        }
    }

    @Test public void testDatasetStore() {
        try {
            final RandomDatasetInfoSet before = new RandomDatasetInfoSet(4096);

            /* Create the store and verify that it has everything in it. */
            try (DatasetInfoStore store = new DatasetInfoStore(new File(diDir))) {
                    for (DatasetInfo shardInfo: before) {
                        store.put(shardInfo);
                    }

                    HashSet<DatasetInfo> after = new HashSet<DatasetInfo>();
                    Iterator<DatasetInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }
                    assertEquals(before, after);
                }

            /* Reopen the store and verify that it has everything in it. */
            try (DatasetInfoStore store = new DatasetInfoStore(new File(diDir))) {
                    HashSet<DatasetInfo> after = new HashSet<DatasetInfo>();
                    Iterator<DatasetInfo> it = store.iterator();
                    while (it.hasNext()) {
                        after.add(it.next());
                    }
                    assertEquals(before, after);
                }
        }
        catch (IOException ex) {
            fail(ex.toString());
        }
    }

    private ObjectArrayList<String> newRandomFieldList() {
        final int numFields = rng.nextInt(256);
        final ObjectArrayList<String> result = new ObjectArrayList<String>(numFields);
        for (int count = 0; count < numFields; ++count) {
            final String field = RandomStringUtils.randomAlphanumeric(8 + rng.nextInt(32));
            result.add(field);
        }
        return result;
    }

    private class RandomShardInfoSet extends HashSet<ShardInfo> {
        RandomShardInfoSet(int size) {
            super(size);
            for (int count = 0; count < size; ++count) {
                add(new ShardInfo(RandomStringUtils.randomAlphanumeric(32),
                                  RandomStringUtils.randomAlphanumeric(rng.nextInt(256)),
                                  null, rng.nextInt(), rng.nextLong()));
            }
        }
    }

    private class RandomDatasetInfoSet extends HashSet<DatasetInfo> {
        RandomDatasetInfoSet(int size) {
            for (int count = 0; count < size; ++count) {
            final String dataset = RandomStringUtils.randomAlphanumeric(16);
            final ObjectArrayList<ShardInfo> sis = new ObjectArrayList<ShardInfo>(0);
            final ObjectArrayList<String> intFields = newRandomFieldList();
            final ObjectArrayList<String> strFields = newRandomFieldList();
            add(new DatasetInfo(dataset, sis, intFields, strFields, intFields));
            }
        }
    }
}
