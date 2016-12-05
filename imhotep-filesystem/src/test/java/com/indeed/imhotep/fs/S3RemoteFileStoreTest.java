package com.indeed.imhotep.fs;

import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import static com.indeed.imhotep.fs.RemoteCachingFileSystemTestUtils.readFromPath;

/**
 * @author kenh
 */
@Ignore("No S3 credentials to run it")
public class S3RemoteFileStoreTest {
    @Rule
    public RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext(ImmutableMap.of(
            "imhotep.fs.store.type", "s3"
    ));

    private void writeToFile(final Path path, final String...lines) {
        final StringBuilder contentsBuilder = new StringBuilder();
        for (final String line : lines) {
            contentsBuilder.append(line).append('\n');
        }
        testContext.putS3File(path, contentsBuilder.toString());
    }

    @Test
    public void testFileReplication() throws IOException {
        writeToFile(Paths.get("a", "aa1"), "this is a test", "here is another test");

        writeToFile(Paths.get("b", "c", "bc1"), "bc1");
        writeToFile(Paths.get("b", "c", "bc2"), "bc2");
        writeToFile(Paths.get("b", "c", "bc3"), "bc3");

        writeToFile(Paths.get("b", "cd", "bcd4"), "bcd4");
        writeToFile(Paths.get("b", "ce"), "bce5");

        final FileSystem fs = testContext.getFs();

        // check that we can read data replicated from the store
        try (BufferedReader bufferedReader = Files.newBufferedReader(fs.getPath("/a", "aa1"), Charsets.UTF_8)) {
            Assert.assertEquals("this is a test", bufferedReader.readLine());
            Assert.assertEquals("here is another test", bufferedReader.readLine());
            Assert.assertNull(bufferedReader.readLine());
        }

        Assert.assertEquals("bc1\n", readFromPath(fs.getPath("/b", "c", "bc1")));
        Assert.assertEquals("bc2\n", readFromPath(fs.getPath("/b", "c", "bc2")));
        Assert.assertEquals("bc3\n", readFromPath(fs.getPath("/b", "c", "bc3")));

        final BasicFileAttributes bAttr = Files.readAttributes(fs.getPath("b"), BasicFileAttributes.class);
        Assert.assertTrue(bAttr.isDirectory());

        final BasicFileAttributes cAttr = Files.readAttributes(fs.getPath("b", "c"), BasicFileAttributes.class);
        Assert.assertTrue(cAttr.isDirectory());

        final BasicFileAttributes bc1Attr = Files.readAttributes(fs.getPath("b", "c", "bc1"), BasicFileAttributes.class);
        Assert.assertFalse(bc1Attr.isDirectory());
        Assert.assertEquals(4, bc1Attr.size());

        final BasicFileAttributes bc2Attr = Files.readAttributes(fs.getPath("b", "c", "bc2"), BasicFileAttributes.class);
        Assert.assertFalse(bc2Attr.isDirectory());
        Assert.assertEquals(4, bc2Attr.size());

        Assert.assertEquals(Sets.newHashSet(
                fs.getPath("b", "c"),
                fs.getPath("b", "cd"),
                fs.getPath("b", "ce")
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("b"))).toSet());

        Assert.assertEquals(Sets.newHashSet(
                fs.getPath("b", "c", "bc1"),
                fs.getPath("b", "c", "bc2"),
                fs.getPath("b", "c", "bc3")
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("b", "c"))).toSet());
    }

    @Test(expected = NoSuchFileException.class)
    public void readAttributeOnMissingFile1() throws IOException {
        writeToFile(Paths.get("b", "cd", "bcd4"), "bcd4");
        writeToFile(Paths.get("b", "ce"), "bce5");

        final FileSystem fs = testContext.getFs();
        Files.readAttributes(fs.getPath("b", "c"), BasicFileAttributes.class);
    }

    @Test(expected = NoSuchFileException.class)
    public void readAttributeOnMissingFile2() throws IOException {
        writeToFile(Paths.get("b", "cd", "bcd4"), "bcd4");
        writeToFile(Paths.get("b", "ce"), "bce5");

        final FileSystem fs = testContext.getFs();
        Files.readAttributes(fs.getPath("b", "cd", "b"), BasicFileAttributes.class);
    }

    @Test(expected = NotDirectoryException.class)
    public void testListDirOnFile() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        writeToFile(Paths.get("a", "aa1"), "this is a test", "here is another test");

        Assert.assertEquals(Sets.newHashSet(
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("a", "aa1"))).toSet());
    }

    @Test(expected = NoSuchFileException.class)
    public void testListDirOnMissingPath() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        Assert.assertEquals(Sets.newHashSet(
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("a"))).toSet());
    }
}