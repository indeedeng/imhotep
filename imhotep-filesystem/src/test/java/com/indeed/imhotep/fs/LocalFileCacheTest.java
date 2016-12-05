package com.indeed.imhotep.fs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.indeed.util.io.Files;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kenh
 */

public class LocalFileCacheTest {
    @Rule
    public RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext();

    private static String generateFileData(final RemoteCachingPath path, final int size) {
        return RandomStringUtils.random(size - 1, 0, 0, true, true, null, new Random(path.hashCode()));
    }

    static class DiskUsageCounter extends SimpleFileVisitor<Path> {
        private long totalSize = 0;

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            totalSize += java.nio.file.Files.size(file);
            return super.visitFile(file, attrs);
        }

        public long getTotalSize() {
            return totalSize;
        }
    }

    private static long getCacheUsage(final Path cacheDir) throws IOException {
        final DiskUsageCounter diskUsageCounter = new DiskUsageCounter();
        java.nio.file.Files.walkFileTree(cacheDir, diskUsageCounter);
        return diskUsageCounter.getTotalSize();
    }

    private static String readPath(final Path cachePath) {
        return Files.readTextFile(cachePath.toString())[0];
    }

    static class RandomCacheFileLoader implements LocalFileCache.CacheFileLoader {
        private final int fileSize;
        private final AtomicInteger loadCount = new AtomicInteger(0);

        RandomCacheFileLoader(final int fileSize) {
            this.fileSize = fileSize;
        }

        @Override
        public void load(final RemoteCachingPath src, final Path dest) throws IOException {
            Assert.assertFalse(java.nio.file.Files.exists(dest));
            loadCount.incrementAndGet();
            final String payload = generateFileData(src, fileSize);
            Files.writeToTextFileOrDie(new String[]{payload}, dest.toString());
        }
    }

    @Test
    public void testDuplicateLoads() throws IOException, ExecutionException {
        final int fileSize = 128;
        final int maxEntries = 8;
        // the strategy for the guava cache is to divide the max size into segments (set by concurrency level) and
        // bound each segments by maxCapacity / segments. By default this is 4.
        // To ensure everything stays in the cache, we increase the max capacity by 4.
        final int maxCapacity = maxEntries * fileSize * 4;

        final RemoteCachingFileSystem fs = testContext.getFs();
        final RemoteCachingPath rootPath = RemoteCachingPath.getRoot(fs);
        final Path cacheBasePath = testContext.getCacheDir().toPath();

        final RandomCacheFileLoader cacheFileLoader = new RandomCacheFileLoader(fileSize);
        final LocalFileCache localFileCache = new LocalFileCache(fs, cacheBasePath, maxCapacity, cacheFileLoader);

        Assert.assertEquals(0, getCacheUsage(cacheBasePath));
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

        // cache the same files over and over again. Ensure they do not get evicted.
        for (int i = 1; i <= (maxEntries * 10); i++) {
            final RemoteCachingPath file = rootPath.resolve("cachedOnly").resolve("cacheOnly." + (i % maxEntries) + ".file");
            Assert.assertEquals(generateFileData(file, fileSize), readPath(localFileCache.cache(file)));
            Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
            Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
        }
        Assert.assertEquals(maxEntries, cacheFileLoader.loadCount.get());
    }

    @Test
    public void testConcurrentStreams() throws IOException, ExecutionException, InterruptedException {
        final int fileSize = 128;
        final int maxEntries = 8;
        final int numThreads = 10;
        final int numIterations = 1024 * 32;

        final int maxCapacity = maxEntries * fileSize;

        final RemoteCachingFileSystem fs = testContext.getFs();
        final RemoteCachingPath rootPath = RemoteCachingPath.getRoot(fs);
        final Path cacheBasePath = testContext.getCacheDir().toPath();

        final RandomCacheFileLoader cacheFileLoader = new RandomCacheFileLoader(fileSize);
        final LocalFileCache localFileCache = new LocalFileCache(fs, cacheBasePath, maxCapacity, cacheFileLoader);

        Assert.assertEquals(0, getCacheUsage(cacheBasePath));

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final List<ListenableFutureTask<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            final ListenableFutureTask<Void> openTask = ListenableFutureTask.create(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int i = 1; i <= numIterations; i++) {
                        final RemoteCachingPath file = rootPath.resolve("opened").resolve("opened." + (i % maxEntries * 2) + ".file");
                        try (LocalFileCache.ScopedCacheFile openedFile = localFileCache.getForOpen(file)) {
                            Assert.assertEquals(generateFileData(file, fileSize), readPath(openedFile.getCachePath()));
                        }
                    }
                    return null;
                }
            });
            tasks.add(openTask);
            executorService.submit(openTask);

            final ListenableFutureTask<Void> cacheTask = ListenableFutureTask.create(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int i = 1; i <= numIterations; i++) {
                        final RemoteCachingPath file = rootPath.resolve("cacheOnly").resolve("cacheOnly." + (i % maxEntries * 2) + ".file");
                        localFileCache.cache(file);
                    }
                    return null;
                }
            });
            tasks.add(cacheTask);
            executorService.submit(cacheTask);
        }

        Futures.allAsList(tasks).get();

        Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
    }

    @Test
    public void testCacheOpenAndEvict() throws IOException, ExecutionException {
        final int fileSize = 128;
        final int maxEntries = 8;
        final int maxCapacity = maxEntries * fileSize;

        final RemoteCachingFileSystem fs = testContext.getFs();
        final RemoteCachingPath rootPath = RemoteCachingPath.getRoot(fs);
        final Path cacheBasePath = testContext.getCacheDir().toPath();

        final RandomCacheFileLoader cacheFileLoader = new RandomCacheFileLoader(fileSize);
        final LocalFileCache localFileCache = new LocalFileCache(fs, cacheBasePath, maxCapacity, cacheFileLoader);

        Assert.assertEquals(0, getCacheUsage(cacheBasePath));
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

        // fill up the cache, and have some entries evicted along the way
        int cacheCount = 0;
        for (int i = 1; i <= (maxEntries * 10); i++) {
            final RemoteCachingPath file = rootPath.resolve("cachedOnly").resolve("cacheOnly." + (i % (maxEntries * 2)) + ".file");
            Assert.assertEquals(generateFileData(file, fileSize), readPath(localFileCache.cache(file)));
            Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
            ++cacheCount;
        }
        Assert.assertEquals(cacheCount, cacheFileLoader.loadCount.get());

        final List<LocalFileCache.ScopedCacheFile> scopedCacheFiles = new ArrayList<>();

        // open some files
        for (int i = 1; i <= (maxEntries * 3); i++) {
            final RemoteCachingPath file = rootPath.resolve("opened").resolve("opened." + i + ".file");
            final LocalFileCache.ScopedCacheFile openedFile = localFileCache.getForOpen(file);
            Assert.assertEquals(generateFileData(file, fileSize), readPath(openedFile.getCachePath()));
            scopedCacheFiles.add(openedFile);
            ++cacheCount;
        }
        Assert.assertEquals(cacheCount, cacheFileLoader.loadCount.get());

        // try to cache some files again, but they should be evicted immediately
        for (int i = 1; i <= (maxEntries * 10); i++) {
            final RemoteCachingPath file = rootPath.resolve("cachedOnly2").resolve("cacheOnly2." + (i % (maxEntries * 2)) + ".file");
            final Path cachedFile = localFileCache.cache(file);
            Assert.assertTrue(java.nio.file.Files.notExists(cachedFile));
            ++cacheCount;
        }
        Assert.assertEquals(cacheCount, cacheFileLoader.loadCount.get());

        // ensure all opened files are in the cache
        for (final LocalFileCache.ScopedCacheFile scopedCacheFile : scopedCacheFiles) {
            Assert.assertTrue(java.nio.file.Files.exists(scopedCacheFile.getCachePath()));
        }

        // the total usage is above threshold because we have both cached and opened files
        Assert.assertTrue(getCacheUsage(cacheBasePath) > maxCapacity);
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

        final List<LocalFileCache.ScopedCacheFile> moreScopedCacheFiles = new ArrayList<>();

        // open the same files again
        // the load count shouldn't change
        for (int i = 1; i <= (maxEntries * 2); i++) {
            final RemoteCachingPath file = rootPath.resolve("opened").resolve("opened." + i + ".file");
            final LocalFileCache.ScopedCacheFile openedFile = localFileCache.getForOpen(file);
            Assert.assertEquals(generateFileData(file, fileSize), readPath(openedFile.getCachePath()));
            moreScopedCacheFiles.add(openedFile);
        }
        Assert.assertEquals(cacheCount, cacheFileLoader.loadCount.get());

        // close all opened files and ensure that the cache directory space goes back down
        for (final LocalFileCache.ScopedCacheFile scopedCacheFile : scopedCacheFiles) {
            scopedCacheFile.close();
        }

        // we still have open files should should be above threshold
        Assert.assertTrue(getCacheUsage(cacheBasePath) > maxCapacity);
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

        // close all opened files and ensure that the cache directory space goes back below the threshold
        for (final LocalFileCache.ScopedCacheFile scopedCacheFile : moreScopedCacheFiles) {
            scopedCacheFile.close();
        }

        // now that all opened files are closed, the cache directory usage should be below the threshold
        Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

        // try to cache some files again, but they should now be cached
        for (int i = 1; i <= (maxEntries * 10); i++) {
            final RemoteCachingPath file = rootPath.resolve("cachedOnly3").resolve("cacheOnly3." + (i % (maxEntries * 2)) + ".file");
            final Path cachedFile = localFileCache.cache(file);
            Assert.assertEquals(generateFileData(file, fileSize), readPath(cachedFile));
            Assert.assertTrue(java.nio.file.Files.exists(cachedFile));
            ++cacheCount;
        }
        Assert.assertEquals(cacheCount, cacheFileLoader.loadCount.get());

        // we still have open files should should be above threshold
        Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
        Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
    }

    @Test
    public void testCacheRecovery() throws IOException, ExecutionException {
        final int fileSize = 128;
        final int maxEntries = 8;
        final int maxCapacity = maxEntries * fileSize;

        final RemoteCachingFileSystem fs = testContext.getFs();
        final RemoteCachingPath rootPath = RemoteCachingPath.getRoot(fs);
        final Path cacheBasePath = testContext.getCacheDir().toPath();

        {
            final LocalFileCache localFileCache = new LocalFileCache(fs, cacheBasePath, maxCapacity, new RandomCacheFileLoader(fileSize));

            Assert.assertEquals(0, getCacheUsage(cacheBasePath));
            Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

            // fill up the cache, and have some entries evicted along the way
            for (int i = 1; i <= (maxEntries * 10); i++) {
                final RemoteCachingPath file = rootPath.resolve("cachedOnly").resolve("cacheOnly." + (i % (maxEntries * 2)) + ".file");
                Assert.assertEquals(generateFileData(file, fileSize), readPath(localFileCache.cache(file)));
                Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
                Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
            }

            final List<LocalFileCache.ScopedCacheFile> scopedCacheFiles = new ArrayList<>();

            // open some files
            for (int i = 1; i <= (maxEntries * 2); i++) {
                final RemoteCachingPath file = rootPath.resolve("opened").resolve("opened." + i + ".file");
                final LocalFileCache.ScopedCacheFile openedFile = localFileCache.getForOpen(file);
                Assert.assertEquals(generateFileData(file, fileSize), readPath(openedFile.getCachePath()));
                scopedCacheFiles.add(openedFile);
            }

            // ensure all opened files are in the cache
            for (final LocalFileCache.ScopedCacheFile scopedCacheFile : scopedCacheFiles) {
                Assert.assertTrue(java.nio.file.Files.exists(scopedCacheFile.getCachePath()));
            }
            // do not close it
            Assert.assertTrue(getCacheUsage(cacheBasePath) > maxCapacity);
            Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
        }

        {
            // reinitialize the cache
            final LocalFileCache localFileCache = new LocalFileCache(fs, cacheBasePath, maxCapacity, new RandomCacheFileLoader(fileSize));

            // all files from previous opened/closed files should be treated as closed
            // so the cache usage should be below the threshold
            Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
            Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());

            final List<LocalFileCache.ScopedCacheFile> scopedCacheFiles = new ArrayList<>();

            // open some files
            for (int i = 1; i <= (maxEntries * 2); i++) {
                final RemoteCachingPath file = rootPath.resolve("opened").resolve("opened." + i + ".file");
                final LocalFileCache.ScopedCacheFile openedFile = localFileCache.getForOpen(file);
                Assert.assertEquals(generateFileData(file, fileSize), readPath(openedFile.getCachePath()));
                scopedCacheFiles.add(openedFile);
            }

            // fill up the cache, and have some entries evicted along the way
            // we want to test if opened files are evicted
            for (int i = 1; i <= (maxEntries * 10); i++) {
                final RemoteCachingPath file = rootPath.resolve("cachedOnly").resolve("cacheOnly." + (i % (maxEntries * 2)) + ".file");
                localFileCache.cache(file);
            }

            // ensure all opened files are in the cache
            for (final LocalFileCache.ScopedCacheFile scopedCacheFile : scopedCacheFiles) {
                Assert.assertTrue(java.nio.file.Files.exists(scopedCacheFile.getCachePath()));
            }

            // close all opened files and ensure that the cache directory space goes back below the threshold
            for (final LocalFileCache.ScopedCacheFile scopedCacheFile : scopedCacheFiles) {
                scopedCacheFile.close();
            }

            // now that all opened files are closed, the cache directory usage should be below the threshold
            Assert.assertTrue(getCacheUsage(cacheBasePath) <= maxCapacity);
            Assert.assertEquals(getCacheUsage(cacheBasePath), localFileCache.getCacheUsage());
        }
    }
}