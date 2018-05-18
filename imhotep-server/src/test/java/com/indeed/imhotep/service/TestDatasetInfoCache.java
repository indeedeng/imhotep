/*
 * Copyright (C) 2018 Indeed Inc.
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
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDatasetInfoCache {
    private static final Random rng = new Random(0xdeadbeef);

    private LocalImhotepServiceConfig config = null;
    private List<SkeletonDataset>     sds    = null;

    private Path datasetDir = null;
    private Path storeDir   = null;
    private Path unusedDir  = null;

    @Before public void setUp() throws IOException {
        config     = new LocalImhotepServiceConfig();
        datasetDir = Files.createTempDirectory("Datasets.");
        storeDir   = Files.createTempDirectory("ShardStore.");
        unusedDir  = Files.createTempDirectory("Unused.");
        sds        = generateDatasets(16);
    }

    @After public void tearDown() throws IOException {
        TestFileUtils.deleteDirTree(datasetDir);
        TestFileUtils.deleteDirTree(storeDir);
        TestFileUtils.deleteDirTree(unusedDir);
    }

    /** Verify that LocalImhotepServiceCore can build a ShardMap from datasets
     *  on the filesystem.
     */
    @Test public void testLoadFromFilesystem() {
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
    }

    /** Verify that LocalImhotepServiceCore can load a ShardMap from a
     *  ShardStore cache.
     */
    @Test public void testLoadFromCache() {
        /* first time round, we create the cache... */
        checkServiceCore(ShardUpdateListenerIf.Source.FILESYSTEM);
        /* second time we should load it */
        checkServiceCore(ShardUpdateListenerIf.Source.CACHE);
    }

    /** Verify that we correctly fall back to scraping the filesystem in the
     *  face of a corrupt cache.
     */
    @Test public void testFallback() throws IOException {
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

        public CheckDatasetUpdate(final List<SkeletonDataset> expected,
                                  final Source expectedSource) {
            this.expected       = expected;
            this.expectedSource = expectedSource;
        }

        public void onShardUpdate(final List<ShardInfo> shardList, final Source source) { }
        public void onDatasetUpdate(final List<DatasetInfo> datasetList, final Source source) {
            sourcesMatch.set(source == expectedSource);
            datasetsMatch.set(compare(expected, datasetList));
        }

        private static boolean compare(final Collection<String> thing1, final Collection<String> thing2) {
            if (thing1.size() != thing2.size()) {
                return false;
            }
            final List<String> list1 = new ArrayList<>(thing1);
            final List<String> list2 = new ArrayList<>(thing2);
            Collections.sort(list1);
            Collections.sort(list2);
            return list1.equals(list2);
        }

        private static boolean compare(
                final List<SkeletonDataset> expected,
                final List<DatasetInfo> actual) {

            if (expected.size() != actual.size()) {
                return false;
            }

            Collections.sort(expected, new Comparator<SkeletonDataset>() {
                    public int compare(final SkeletonDataset thing1, final SkeletonDataset thing2) {
                        return thing1.getDatasetDir().compareTo(thing2.getDatasetDir());
                    }
                });


            Collections.sort(actual, new Comparator<DatasetInfo>() {
                    public int compare(final DatasetInfo thing1, final DatasetInfo thing2) {
                        return thing1.getDataset().compareTo(thing2.getDataset());
                    }
                });

            final Iterator<SkeletonDataset> expectedIt = expected.iterator();
            final Iterator<DatasetInfo>     actualIt   = actual.iterator();
            while (expectedIt.hasNext() && actualIt.hasNext()) {
                final SkeletonDataset sd = expectedIt.next();
                final DatasetInfo     di = actualIt.next();
                if (! sd.getDatasetDir().getFileName().toString().equals(di.getDataset())) {
                    return false;
                }
                if (sd.getNumShards() != di.getShardList().size()) {
                    return false;
                }
                if (! compare(Arrays.asList(sd.getIntFieldNames()), di.getIntFields())) {
                    return false;
                }
                if (! compare(Arrays.asList(sd.getStrFieldNames()), di.getStringFields())) {
                    return false;
                }
            }
            return true;
        }
    }

    private List<SkeletonDataset> generateDatasets(int numDatasets)
        throws IOException {
        final List<SkeletonDataset> result = new ArrayList<>(numDatasets);
        while (--numDatasets >= 0) {
            final int maxNumShards = Math.max(rng.nextInt(256), 16);
            final int maxNumDocs   = Math.max(rng.nextInt(1000000), 1);
            final int maxNumFields = Math.max(rng.nextInt(50), 1);
            final SkeletonDataset sd =
                new SkeletonDataset(rng, datasetDir,
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
            svcCore = new LocalImhotepServiceCore(datasetDir,
                                                  unusedDir,
                                                  storeDir,
                                                  500,
                                                  new GenericFlamdexReaderSource(),
                                                  new ShardDirIteratorFactory(null, null),
                                                  config,
                                                  listener,
                                                  MetricStatsEmitter.NULL_EMITTER);
            assertTrue("loaded DatasetInfo from " + source.name(),
                       listener.sourcesMatch.get());
            assertTrue("DatasetInfo lists match",
                       listener.datasetsMatch.get());
        }
        catch (final IOException ex) {
            fail(ex.toString());
        }
        finally {
            if (svcCore != null) {
                svcCore.close();
            }
        }
    }

    /** This method takes hamfisted approach to corrupting an LSM tree store,
     * looking for its 'latest' and truncating all the files it contains. */
    private void injectCacheCorruption() throws IOException {
        final Path cacheDir = storeDir;
        for (final Path child : Files.newDirectoryStream(cacheDir)) {
            if (child.getFileName().toString().equals("latest") && Files.isDirectory(child)) {
                for (final Path latestChild : Files.newDirectoryStream(child)) {
                    try (SeekableByteChannel oc = Files.newByteChannel(latestChild,
                                                                       StandardOpenOption.WRITE)) {
                        oc.truncate(1);
                    }
                }
            }
        }
    }
}
