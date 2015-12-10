package com.indeed.imhotep.service;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.util.io.Files;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

public class TestDatasetInfoStore {

    private static final ObjectArrayList<ShardInfo> emptyShardInfo =
        new ObjectArrayList<ShardInfo>(0);

    private static final Random rng = new Random(0xdeadbeef);

    @Test public void testRoundTrip() {

        String tmp = null;
        try {
            tmp = Files.getTempDirectory(TestDatasetInfoStore.class.getSimpleName(),
                                         "delete.me");

            HashSet<DatasetInfo> before = new HashSet<DatasetInfo>();
            for (int count = 0; count < 4096; ++count) {
                before.add(newRandomDatasetInfo());
            }

            /* Create the store and verify that it has everything in it. */
            try (DatasetInfoStore store = new DatasetInfoStore(new File(tmp))) {
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
            try (DatasetInfoStore store = new DatasetInfoStore(new File(tmp))) {
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
        finally {
            if (tmp != null) Files.delete(tmp);
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

    private DatasetInfo newRandomDatasetInfo() {
        final String dataset = RandomStringUtils.randomAlphanumeric(16);
        final ObjectArrayList<String> intFields = newRandomFieldList();
        final ObjectArrayList<String> strFields = newRandomFieldList();
        return new DatasetInfo(dataset, emptyShardInfo, intFields, strFields, intFields);
    }
}
