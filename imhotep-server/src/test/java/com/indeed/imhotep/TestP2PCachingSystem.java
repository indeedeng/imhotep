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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

/**
 * @author xweng
 */
public class TestP2PCachingSystem {
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
    public void testLocalPath() throws IOException {
        final Path localFilePath = rootPath.resolve(INDEX_DIR_PREFIX).resolve("fld-if1.intdocs");
        assertTrue(Files.exists(localFilePath));
    }

    @Test
    public void testRemotePath() throws IOException {
        final Path localFilePath = rootPath.resolve(INDEX_DIR_PREFIX).resolve("fld-if1.intdocs");

        final Path fakeRemotePath = rootPath
                .resolve("remote")
                .resolve("localhost:" + testContext.getDaemonPort())
                .resolve(INDEX_DIR_PREFIX)
                .resolve("fld-if1.intdocs");
        final Path remotePath = Paths.get(fakeRemotePath.toUri());
        assertTrue(FileUtils.contentEquals(localFilePath.toFile(), remotePath.toFile()));
    }
}

