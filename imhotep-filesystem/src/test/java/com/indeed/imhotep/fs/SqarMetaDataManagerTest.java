package com.indeed.imhotep.fs;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author kenh
 */
public class SqarMetaDataManagerTest {
    @Rule
    public RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext(new File(getClass().getResource("/").getFile()));

    @Test
    public void testSqarExists() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        testSqarExistsWithDir(root.resolve("testData/test-archive"));
        testSqarExistsWithDir(root.resolve("testData/test-archive.sqar"));
    }

    private void testSqarExistsWithDir(final RemoteCachingPath testArchive) throws IOException, URISyntaxException {
        for (final RemoteCachingPath dirPath : Arrays.asList(
                testArchive.resolve("1"),
                testArchive.resolve("1").resolve("2").resolve("3"),
                testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("5")
        )) {
            assertTrue(Files.exists(dirPath));
            assertTrue(Files.isDirectory(dirPath));
            assertTrue(Files.isRegularFile(dirPath));
        }

        for (final RemoteCachingPath filePath : Arrays.asList(
                testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("1234.file"),
                testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("5").resolve("12345.file"),
                testArchive.resolve("3").resolve("4").resolve("5").resolve("345.file"),
                testArchive.resolve("4").resolve("5").resolve("45.file")
        )) {
            assertTrue(Files.exists(filePath));
            assertFalse(Files.isDirectory(filePath));
            assertTrue(Files.isRegularFile(filePath));
        }

        for (final RemoteCachingPath missingPath : Arrays.asList(
                testArchive.resolve("6"),
                testArchive.resolve("1").resolve("12345.file"),
                testArchive.resolve("4").resolve("5").resolve("54.file")
        )) {
            assertFalse(Files.exists(missingPath));
            assertFalse(Files.isDirectory(missingPath));
            assertFalse(Files.isRegularFile(missingPath));
        }
    }

    @Test
    public void testListRootDirectories() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        Assert.assertEquals(
                ImmutableSet.of(
                        root.resolve("com"),
                        root.resolve("testData")
                ), FluentIterable.from(Files.newDirectoryStream(root, new DirectoryStream.Filter<Path>() {
                    @Override
                    public boolean accept(final Path entry) throws IOException {
                        return Files.isDirectory(entry);
                    }
                })).toSet()
        );

        final RemoteCachingPath indexDir = root.resolve("testData");
        Assert.assertEquals(
                ImmutableSet.of(
                        indexDir.resolve("test-archive")
                ), FluentIterable.from(Files.newDirectoryStream(indexDir)).toSet()
        );
    }

    @Test
    public void testSqarListDirectory() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        testSqarListDirectoryWithDir(root.resolve("testData/test-archive"));
        testSqarListDirectoryWithDir(root.resolve("testData/test-archive.sqar"));
    }

    private void testSqarListDirectoryWithDir(final RemoteCachingPath testArchive) throws IOException, URISyntaxException {
        {
            final RemoteCachingPath dirPath = testArchive.resolve("1");
            Files.newDirectoryStream(dirPath);
            Assert.assertEquals(
                    ImmutableSet.of(
                            dirPath.resolve("1"),
                            dirPath.resolve("2"),
                            dirPath.resolve("1.file")
                    ), FluentIterable.from(Files.newDirectoryStream(dirPath)).toSet()
            );
        }

        {
            final RemoteCachingPath dirPath = testArchive.resolve("1").resolve("2").resolve("3");
            Assert.assertEquals(
                    ImmutableSet.of(
                            dirPath.resolve("4"),
                            dirPath.resolve("123.file")
                    ), FluentIterable.from(Files.newDirectoryStream(dirPath)).toSet()
            );
        }

        {
            final RemoteCachingPath dirPath = testArchive.resolve("1");
            Assert.assertEquals(
                    ImmutableSet.of(
                            dirPath.resolve("1"),
                            dirPath.resolve("2"),
                            dirPath.resolve("1.file")
                    ), FluentIterable.from(Files.newDirectoryStream(dirPath)).toSet()
            );
        }

        {
            Assert.assertEquals(
                    ImmutableSet.of(
                            testArchive.resolve("1"),
                            testArchive.resolve("2"),
                            testArchive.resolve("3"),
                            testArchive.resolve("4"),
                            testArchive.resolve("5")
                    ), FluentIterable.from(Files.newDirectoryStream(testArchive)).toSet()
            );
        }
    }

    @Test(expected = NoSuchFileException.class)
    public void testNotExistingSqarListDirectory() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        final RemoteCachingPath testArchive = root.resolve("testData/test-archive");

        FluentIterable.from(Files.newDirectoryStream(testArchive.resolve("4").resolve("5").resolve("54.file"))).toSet();
    }

    @Test(expected = NotDirectoryException.class)
    public void testNotDirSqarListDirectory() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        final RemoteCachingPath testArchive = root.resolve("testData/test-archive");

        FluentIterable.from(Files.newDirectoryStream(testArchive.resolve("4").resolve("5").resolve("45.file"))).toSet();
    }

    @Test
    public void testSqarLoadFile() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        testSqarLoadFileWithDir(root.resolve("testData/test-archive"));
        testSqarLoadFileWithDir(root.resolve("testData/test-archive.sqar"));
    }

    private void testSqarLoadFileWithDir(final RemoteCachingPath testArchive) throws IOException, URISyntaxException {
        final RemoteCachingPath file12345 = testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("5").resolve("12345.file");
        testInputStream(file12345, 12345);

        final RemoteCachingPath file1234 = testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("1234.file");
        testInputStream(file1234, 1234);

        final RemoteCachingPath file45 = testArchive.resolve("4").resolve("5").resolve("45.file");
        testInputStream(file45, 45);
    }

    @Test(expected = IOException.class)
    public void testSqarLoadDirectory() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        final RemoteCachingPath testArchive = root.resolve("testData/test-archive");

        Files.newInputStream(testArchive.resolve("1").resolve("2").resolve("3").resolve("4").resolve("5"));
    }

    @Test(expected = IOException.class)
    public void testSqarLoadMissing() throws IOException, URISyntaxException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final RemoteCachingPath root = RemoteCachingPath.getRoot(fs);

        assertTrue(Files.exists(root));
        assertTrue(Files.isDirectory(root));
        assertTrue(Files.isRegularFile(root));

        final RemoteCachingPath testArchive = root.resolve("testData/test-archive");

        Files.newInputStream(testArchive.resolve("abc.file"));
    }

    private void testInputStream(final Path path, final int count) throws IOException {
        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < count; i++) {
            expected.append("foo!");
        }

        final ByteArrayOutputStream actual = new ByteArrayOutputStream();

        final byte[] buffer = new byte[1024];
        final InputStream is = Files.newInputStream(path);

        try (DataInputStream reader = new DataInputStream(is)) {
            int read = reader.read(buffer, 0, buffer.length);
            while (read != -1) {
                actual.write(buffer, 0, read);
                read = reader.read(buffer, 0, buffer.length);
            }
        }

        Assert.assertEquals(expected.toString(), actual.toString());
    }
}
