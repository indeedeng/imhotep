package com.indeed.imhotep;

import com.indeed.imhotep.fs.RemoteCachingPath;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestP2PCachingFile {
    private static P2PCachingTestContext testContext;
    private static RemoteCachingPath rootPath;
    private static final String INDEX_DIR_PREFIX = "data/index20171231.20180301170838";

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        testContext = new P2PCachingTestContext();
        testContext.createIndex(INDEX_DIR_PREFIX, false);
        rootPath = (RemoteCachingPath) testContext.getRootPath();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        testContext.close();
    }

    @Test
    public void testLocalPath() {
        final Path localFilePath = rootPath.resolve(INDEX_DIR_PREFIX).resolve("fld-if1.intdocs");
        assertTrue(Files.exists(localFilePath));
    }

    @Test
    public void testRemotePath() throws IOException {
        assertTrue(internalTestRemote("fld-if1.intdocs"));
    }

    @Test
    public void testRemotePathConcurrently() throws IOException {
        final List<String> fileList = testContext.getIndexFileNames(INDEX_DIR_PREFIX);
        final ExecutorService executor = Executors.newFixedThreadPool(4);
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
        final Path localFilePath = rootPath.resolve(INDEX_DIR_PREFIX).resolve(fileName);
        final Path fakeRemotePath = rootPath
                .resolve("remote")
                .resolve("localhost:" + testContext.getDaemonPort())
                .resolve(INDEX_DIR_PREFIX)
                .resolve(fileName);
        final Path remotePath = Paths.get(fakeRemotePath.toUri());
        return FileUtils.contentEquals(localFilePath.toFile(), remotePath.toFile());
    }
}

