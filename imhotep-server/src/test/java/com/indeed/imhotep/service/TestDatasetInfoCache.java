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

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.util.io.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestDatasetInfoCache {
    private static final Random rng = new Random(0xdeadbeef);

    private LocalImhotepServiceConfig config = null;
    private List<SkeletonDataset>     sds    = null;

    private String datasetDir = null;
    private String storeDir   = null;
    private String unusedDir  = null;

    @Before public void setUp() throws Exception {
        config     = new LocalImhotepServiceConfig();
        datasetDir = Files.getTempDirectory("Datasets.", ".delete.me");
        storeDir   = Files.getTempDirectory("ShardStore.", ".delete.me");
        unusedDir  = Files.getTempDirectory("Unused.", ".delete.me");
        sds        = generateDatasets(16);
    }

    @After public void tearDown() throws Exception {
        Files.delete(datasetDir);
        Files.delete(storeDir);
        Files.delete(unusedDir);
    }

    /** Verify that LocalImhotepServiceCore can build a ShardMap from datasets
     *  on the filesystem.
     */
    @Test public void testLoadFromFilesystem() throws Exception {
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
    }

    /** Verify that LocalImhotepServiceCore can load a ShardMap from a
     *  ShardStore cache.
     */
    @Test public void testLoadFromCache() throws Exception {
        /* first time round, we create the cache... */
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
        /* second time we should load it */
        checkServiceCore(ShardUpdateListenerIf.Source.CACHE);
    }

    /** Verify that we correctly fall back to scraping the filesystem in the
     *  face of a corrupt cache.
     */
    @Test public void testFallback() throws Exception {
        /* first time round, we create the cache... */
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
        injectCacheCorruption();
        /* second time we should fail to load the cache... */
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
        /* third time we should have fixed the cache */
        checkServiceCore(ShardUpdateListenerIf.Source.CACHE);
    }

    private static class CheckDatasetUpdate implements ShardUpdateListenerIf {

        public final AtomicBoolean sourcesMatch = new AtomicBoolean(false);
        public final AtomicBoolean datasetsMatch = new AtomicBoolean(false);

        private final List<SkeletonDataset> expected;
        private final Source expectedSource;

        public CheckDatasetUpdate(List<SkeletonDataset> expected,
                                  Source expectedSource) {
            this.expected       = expected;
            this.expectedSource = expectedSource;
        }

        public void onShardUpdate(final List<ShardInfo> shardList, final Source source) { }
        public void onDatasetUpdate(final List<DatasetInfo> datasetList, final Source source) {
            sourcesMatch.set(source == expectedSource);
            datasetsMatch.set(compare(expected, datasetList));
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
                });


            Collections.sort(actual, new Comparator<DatasetInfo>() {
                    public int compare(DatasetInfo thing1, DatasetInfo thing2) {
                        return thing1.getDataset().compareTo(thing2.getDataset());
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

    private List<SkeletonDataset> generateDatasets(int numDatasets)
        throws Exception {
        List<SkeletonDataset> result = new ArrayList<>(numDatasets);
        while (--numDatasets >= 0) {
            final int maxNumShards = Math.max(rng.nextInt(256), 16);
            final int maxNumDocs   = Math.max(rng.nextInt(1000000), 1);
            final int maxNumFields = Math.max(rng.nextInt(50), 1);
            final SkeletonDataset sd =
                new SkeletonDataset(rng, new File(datasetDir),
                                    maxNumShards, maxNumDocs, maxNumFields);
            /*
            System.err.println("created dataset: " + sd.getDatasetDir() +
                               " numIntFields: " + sd.getIntFieldNames().length +
                               " numStrFields: " + sd.getStrFieldNames().length);
            */
            result.add(sd);
        }
        return result;
    }

    private void checkServiceCore(final ShardUpdateListenerIf.Source source) {
        /*
          System.err.println("trying to load from: " + source.name());
        */
        LocalImhotepServiceCore svcCore = null;
        try {
            final CheckDatasetUpdate listener = new CheckDatasetUpdate(sds, source);
            svcCore = new LocalImhotepServiceCore(datasetDir, unusedDir, storeDir, 500, false,
                                                  new GenericFlamdexReaderSource(), config, listener);
            assertTrue("loaded DatasetInfo from " + source.name(), listener.sourcesMatch.get());
            assertTrue("DatasetInfo lists match", listener.datasetsMatch.get());
        }
        catch (IOException ex) {
            fail(ex.toString());
        }
        finally {
            if (svcCore != null) svcCore.close();
        }
    }

    /** This method takes hamfisted approach to corrupting an LSM tree store,
     * looking for its 'latest' and truncating all the files it contains. */
    private void injectCacheCorruption() throws IOException {
        final File cacheDir = new File(storeDir);
        final File[] children = cacheDir.listFiles();
        if (children != null ) {
            for (final File child: children) {
                if (child.getName().equals("latest") && child.isDirectory()) {
                    for (final File latestChild: child.listFiles()) {
                        try (final FileChannel oc =
                             new FileOutputStream(latestChild, true).getChannel()) {
                            oc.truncate(1);
                        }
                    }
                }
            }
        }
    }
}
