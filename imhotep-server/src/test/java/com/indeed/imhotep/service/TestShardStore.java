package com.indeed.imhotep.service;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.lsmtree.core.Store;
import com.indeed.util.io.Files;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestShardStore {

    private static final Random rng = new Random(0xdeadbeef);

    private String datasetDir = null;
    private String storeDir   = null;
    private String tmpDir     = null;

    @Before public void setUp() throws Exception {
        datasetDir = Files.getTempDirectory("Datasets.", ".delete.me");
        storeDir   = Files.getTempDirectory("ShardStore.", ".delete.me");
        tmpDir     = Files.getTempDirectory("TmpDir.", ".delete.me");
    }

    @After public void tearDown() throws Exception {
        Files.delete(datasetDir);
        Files.delete(storeDir);
        Files.delete(tmpDir);
    }

    @Test public void testShardStore() {
        try {
            RandomEntries before = new RandomEntries(64, 512);

            /* Create the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(new File(storeDir))) {
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
            try (ShardStore store = new ShardStore(new File(storeDir))) {
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

            try (ShardStore store = new ShardStore(new File(storeDir))) {
               for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(1000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(new File(storeDir))) {
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

            try (ShardStore store = new ShardStore(new File(storeDir))) {
               for (Map.Entry<ShardStore.Key, ShardStore.Value> entry: entries.entrySet()) {
                   store.put(entry.getKey(), entry.getValue());
               }
            }

            /* !@# There's currently a bug in LSMTree close() that can cause
               this test to fail if we try to reopen too soon. Until the fix
               for it gets merged, we need this ugly workaround. */
            Thread.sleep(1000);

            /* Reopen the store and verify that it has everything in it. */
            try (ShardStore store = new ShardStore(new File(storeDir))) {
                DatasetInfoList actual = new DatasetInfoList(store);
                /* TODO(johnf): test the content in some meaningful way (without
                 just recreating the DatasetInfoList logic in this test. */
         }
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    private static class CheckDatasetUpdate implements ShardUpdateListenerIf {

        public final AtomicBoolean checkedDataset = new AtomicBoolean(false);

        private final List<SkeletonDataset> expected;

        public CheckDatasetUpdate(List<SkeletonDataset> expected) {
            this.expected = expected;
        }

        public void onShardUpdate(final List<ShardInfo> shardList) { }
        public void onDatasetUpdate(final List<DatasetInfo> datasetList) {
            checkedDataset.set(compare(expected, datasetList));
        }

        private static boolean compare(Collection<String> thing1, Collection<String> thing2) {
            if (thing1.size() != thing2.size()) return false;
            List<String> list1 = new ArrayList<>(thing1);
            List<String> list2 = new ArrayList<>(thing2);
            Collections.sort(list1);
            Collections.sort(list2);
            return list1.equals(list2);
        }

        private static boolean compare(List<SkeletonDataset> expected, List<DatasetInfo> actual) {

            if (expected.size() != actual.size()) return false;

            Collections.sort(expected, new Comparator<SkeletonDataset>() {
                    public int compare(SkeletonDataset thing1, SkeletonDataset thing2) {
                        return thing1.getDatasetDir().compareTo(thing2.getDatasetDir());
                    }
                    public boolean equals(SkeletonDataset thing1, SkeletonDataset thing2) {
                        return thing1.getDatasetDir().equals(thing2.getDatasetDir());
                    }
                });


            Collections.sort(actual, new Comparator<DatasetInfo>() {
                    public int compare(DatasetInfo thing1, DatasetInfo thing2) {
                        return thing1.getDataset().compareTo(thing2.getDataset());
                    }
                    public boolean equals(DatasetInfo thing1, DatasetInfo thing2) {
                        return thing1.getDataset().equals(thing2.getDataset());
                    }
                });

            Iterator<SkeletonDataset> expectedIt = expected.iterator();
            Iterator<DatasetInfo>     actualIt   = actual.iterator();
            while (expectedIt.hasNext() && actualIt.hasNext()) {
                final SkeletonDataset sd = expectedIt.next();
                final DatasetInfo     di = actualIt.next();
                if (! new File(sd.getDatasetDir()).getName().equals(di.getDataset())) return false;
                if (sd.getNumShards() != di.getShardList().size()) return false;
                if (! compare(Arrays.asList(sd.getIntFieldNames()), di.getIntFields())) return false;
                if (! compare(Arrays.asList(sd.getStrFieldNames()), di.getStringFields())) return false;
            }
            return true;
        }
    }

    @Test public void testService() throws Exception {
        final int delaySeconds = 3000;

        final List<SkeletonDataset> sds = generateDatasets(10);

        LocalImhotepServiceConfig config = new LocalImhotepServiceConfig();
        config.setUpdateShardsFrequencySeconds(delaySeconds);
        config.setSyncShardStoreFrequencySeconds(1);

        LocalImhotepServiceCore svcCore = null;
        try {
            final CheckDatasetUpdate listener = new CheckDatasetUpdate(sds);
            svcCore = new LocalImhotepServiceCore(datasetDir, tmpDir, storeDir, 500, false,
                                                  new GenericFlamdexReaderSource(), config, listener);
            assertTrue("scraped DatasetInfo from filesystem", listener.checkedDataset.get());
        }
        finally {
            if (svcCore != null) svcCore.close();
        }

        // !@# TODO(johnf): instrument LocalImhotepServiceCore such
        // !that we can deterministically tell whether we loaded info
        // !list from cache or from filesystem...
        try {
            final CheckDatasetUpdate listener = new CheckDatasetUpdate(sds);
            svcCore = new LocalImhotepServiceCore(datasetDir, tmpDir, storeDir, 500, false,
                                                  new GenericFlamdexReaderSource(), config, listener);
            assertTrue("restored DatasetInfo from cache", listener.checkedDataset.get());
        }
        finally {
            if (svcCore != null) svcCore.close();
        }
    }

    private List<SkeletonDataset> generateDatasets(int numDatasets)
        throws Exception {
        List<SkeletonDataset> result = new ArrayList<SkeletonDataset>(numDatasets);
        while (--numDatasets >= 0) {
            final int maxNumShards = rng.nextInt(100) + 1;
            final int maxNumDocs   = rng.nextInt(1000000) + 1;
            final int maxNumFields = rng.nextInt(50) + 1;
            final SkeletonDataset sd =
                new SkeletonDataset(rng, new File(datasetDir),
                                    maxNumShards, maxNumDocs, maxNumFields);
            System.err.println("created dataset: " + sd.getDatasetDir() +
                               " numIntFields: " + sd.getIntFieldNames().length +
                               " numStrFields: " + sd.getStrFieldNames().length);
            result.add(sd);
        }
        return result;
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
