package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.fs.PeerToPeerCachePath;
import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestPeerToPeerCacheFileStore {
    @Rule
    public PeerToPeerCacheTestContext testContext = new PeerToPeerCacheTestContext();

    private RemoteCachingPath rootPath;
    private RemoteCachingPath shardPath;

    @Before
    public void before() throws IOException {
        testContext.createDailyShard("dataset", 1, false);
        rootPath = (RemoteCachingPath) testContext.getRootPath();
        shardPath = (RemoteCachingPath) testContext.getShardPaths("dataset").get(0);
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

        final List<Path> localListItems;
        try (final DirectoryStream<Path> localDirStream = Files.newDirectoryStream(localFilePath)) {
            localListItems = Lists.newArrayList(localDirStream.iterator());
            Collections.sort(localListItems);
        }

        final List<Path> remoteListItems;
        final Path peerToPeerCachePath = toLocalHostP2PCachingPath(rootPath, localFilePath);
        try (final DirectoryStream<Path> localDirStream = Files.newDirectoryStream(peerToPeerCachePath)) {
            remoteListItems = Lists.newArrayList(localDirStream.iterator());
            Collections.sort(remoteListItems);
        }

        assertNotNull(localListItems);
        assertNotNull(remoteListItems);
        assertEquals(localListItems.size(), remoteListItems.size());
        for (int i = 0; i < localListItems.size(); i++) {
            final Path localPath = localListItems.get(i);
            final Path remotePath = remoteListItems.get(i);

            assertTrue(remotePath instanceof PeerToPeerCachePath);
            assertTrue(localPath instanceof RemoteCachingPath);
            assertEquals(localPath, ((PeerToPeerCachePath) remotePath).getRealPath());
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

    @Test(expected = NoSuchFileException.class)
    public void testListNonexistentDir() throws IOException {
        final Path remotePath = toLocalHostP2PCachingPath(rootPath, shardPath.resolve("nonexistent-dir"));
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(remotePath)) {}
    }

    @Test(expected = NoSuchFileException.class)
    public void testOpenNonexistentRemotePath() throws IOException {
        final Path remotePath = toLocalHostP2PCachingPath(rootPath, shardPath.resolve("nonexistent-file"));
        try(final InputStream fileInputStream = Files.newInputStream(remotePath)) { }
    }

    @Test
    public void testReadNonexistentRemoteAttributes() {
        final Path remotePath = toLocalHostP2PCachingPath(rootPath, shardPath.resolve("nonexistent-dir"));
        assertFalse(Files.exists(remotePath));
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