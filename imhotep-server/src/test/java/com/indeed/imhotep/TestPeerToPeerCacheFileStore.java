package com.indeed.imhotep;

import com.indeed.imhotep.fs.PeerToPeerCachePath;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestPeerToPeerCacheFileStore {
    private static PeerToPeerCacheTestContext testContext;
    private static RemoteCachingPath rootPath;
    private static RemoteCachingPath shardPath;

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        testContext = new PeerToPeerCacheTestContext();
        testContext.createDailyShard("dataset", 1, false);
        rootPath = (RemoteCachingPath) testContext.getRootPath();
        shardPath = (RemoteCachingPath) testContext.getShardPaths("dataset").get(0);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        testContext.close();
    }

    @Test
    public void testLocalPath() {
        final Path localFilePath = shardPath.resolve("fld-if1.intdocs");
        assertTrue(Files.exists(localFilePath));
    }

    /**
     * Cover listDir and getRemoteAttributes
     */
    @Test
    public void testRemotePathListDir() throws IOException {
        final RemoteCachingPath localFilePath = shardPath;
        try (final DirectoryStream<Path> localDirStream = Files.newDirectoryStream(localFilePath)) {
            final Path peerToPeerCachePath = toLocalHostP2PCachingPath(rootPath, localFilePath);
            try (final DirectoryStream<Path> remoteDirStream = Files.newDirectoryStream(peerToPeerCachePath)) {
                Iterator<Path> localIterator = localDirStream.iterator();
                Iterator<Path> remoteIterator = remoteDirStream.iterator();

                while (localIterator.hasNext() && remoteIterator.hasNext()) {
                    final Path nextRemotePath = remoteIterator.next();
                    final Path nextLocalPath = localIterator.next();

                    assertTrue(nextRemotePath instanceof PeerToPeerCachePath);
                    assertTrue(nextLocalPath instanceof RemoteCachingPath);
                    assertEquals(nextLocalPath, ((PeerToPeerCachePath) nextRemotePath).getRealPath());
                }

                if (localIterator.hasNext() || remoteIterator.hasNext()) {
                    fail("The sub files count isn't equal");
                }
            }
        }
    }

    /**
     * Cover getRemoteAttributes and download files
     */
    @Test
    public void testRemotePath() throws IOException {
        assertTrue(internalTestRemote("fld-if1.intdocs"));
        assertTrue(internalTestRemote("fld-shardId.intdocs"));
        assertTrue(internalTestRemote("fld-sf1.strdocs"));
        assertTrue(internalTestRemote("metadata.txt"));
    }

    @Test
    public void testRemotePathConcurrently() throws IOException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final RemoteCachingPath localDirPath = shardPath;

        List<String> fileList;
        try (final DirectoryStream<Path> localDirStream = Files.newDirectoryStream(localDirPath)) {
            fileList = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(localDirStream.iterator(), Spliterator.ORDERED), false)
                    .filter(path -> !Files.isDirectory(path))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        }

        final List<Callable<Boolean>> tasks = IntStream.range(0, 16).mapToObj(i -> new Task(fileList, i)).collect(Collectors.toList());
        try {
            for (Future<Boolean> future : executor.invokeAll(tasks)) {
                assertTrue(future.get());
            }
        } catch (final InterruptedException | ExecutionException e) {
            fail();
        }
    }

    private class Task implements Callable<Boolean> {
        private final int taskIndex;
        private final List<String> fileList;

        Task(final List<String> fileList, final int taskIndex) {
            this.fileList = fileList;
            this.taskIndex = taskIndex;
        }

        @Override
        public Boolean call() throws Exception {
            final String fileName = fileList.get(taskIndex % fileList.size());
            return internalTestRemote(fileName);
        }
    }

    // here it actually downloads files from other server since hostname on machine is username
    private boolean internalTestRemote(final String fileName) throws IOException {
        final RemoteCachingPath localFilePath = shardPath.resolve(fileName);
        final Path remotePath = toLocalHostP2PCachingPath(rootPath, localFilePath);
        return FileUtils.contentEquals(localFilePath.toFile(), remotePath.toFile());
    }

    private PeerToPeerCachePath toLocalHostP2PCachingPath(final RemoteCachingPath rootPath, final RemoteCachingPath localPath) {
        return PeerToPeerCachePath.toPeerToPeerCachePath(rootPath, localPath, testContext.getDaemonHosts().get(0));
    }
}