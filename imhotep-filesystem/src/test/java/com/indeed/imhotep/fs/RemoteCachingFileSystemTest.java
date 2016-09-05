package com.indeed.imhotep.fs;

import com.google.common.base.Charsets;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.indeed.imhotep.fs.RemoteCachingFileSystemTestUtils.readFromPath;
import static com.indeed.imhotep.fs.RemoteCachingFileSystemTestUtils.writeToFile;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemTest {
    @Rule
    public RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext();

    @Test
    public void testPathManipulation() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a file at a");
        writeToFile(new File(storeDir, "rootfile"), "this is a file at root");

        final File bDir = new File(storeDir, "b");
        Assert.assertTrue(bDir.mkdir());
        final File cDir = new File(bDir, "c");
        Assert.assertTrue(cDir.mkdir());
        writeToFile(new File(cDir, "bc1"), "bc1");
        writeToFile(new File(cDir, "bc2"), "bc2");
        writeToFile(new File(cDir, "bc3"), "bc3");

        final FileSystem fs = testContext.getFs();

        Assert.assertEquals(fs.getPath("a"), fs.getPath("a"));

        {
            final Path abc = fs.getPath("abc");
            Assert.assertEquals(abc, abc);

        }

        Assert.assertEquals(fs, fs.getPath("").getFileSystem());

        final Path rootDir = RemoteCachingPath.getRoot((RemoteCachingFileSystem) fs);
        Assert.assertTrue(rootDir.isAbsolute());
        Assert.assertFalse(fs.getPath("").isAbsolute());

        Assert.assertEquals(rootDir, Iterables.getFirst(fs.getRootDirectories(), null));

        Assert.assertTrue(Files.readAttributes(fs.getPath(""), BasicFileAttributes.class).isDirectory());
        Assert.assertTrue(Files.readAttributes(fs.getPath("a"), BasicFileAttributes.class).isDirectory());
        Assert.assertTrue(Files.readAttributes(fs.getPath("b"), BasicFileAttributes.class).isDirectory());
        Assert.assertTrue(Files.readAttributes(fs.getPath("b", "c"), BasicFileAttributes.class).isDirectory());

        Assert.assertFalse(Files.readAttributes(fs.getPath("rootfile"), BasicFileAttributes.class).isDirectory());
        Assert.assertFalse(Files.readAttributes(fs.getPath("a", "aa1"), BasicFileAttributes.class).isDirectory());
        Assert.assertFalse(Files.readAttributes(fs.getPath("b", "c", "bc1"), BasicFileAttributes.class).isDirectory());
        Assert.assertFalse(Files.readAttributes(fs.getPath("b", "c", "bc2"), BasicFileAttributes.class).isDirectory());

        Assert.assertEquals(fs.getPath("c"), fs.getPath("b", "c").getFileName());
        Assert.assertEquals(fs.getPath("bc2"), fs.getPath("b", "c", "bc2").getFileName());

        Assert.assertEquals(fs.getPath("b", "c"), fs.getPath("b", "c", "bc2").getParent());
        Assert.assertEquals(fs.getPath("b"), fs.getPath("b", "c", "bc2").getParent().getParent());
        Assert.assertNull(fs.getPath("b", "c", "bc2").getParent().getParent().getParent());

        Assert.assertEquals(fs.getPath("b", "c", "bc1"), fs.getPath("b", "c", "bc2").resolveSibling("bc1"));

        Assert.assertEquals(fs.getPath("b", "c", "d", "e"), fs.getPath("b", "c", "bc2").getParent().resolve("d").resolve("e"));
        Assert.assertEquals(fs.getPath("b", "c", "bc2"), fs.getPath("b", "c", "bc2").getParent().getParent().resolve("c").resolve("bc2"));

        Assert.assertTrue(fs.getPath("a", "b", "c").startsWith(fs.getPath("a")));
        Assert.assertTrue(fs.getPath("a", "b", "c").startsWith(fs.getPath("a", "b")));
        Assert.assertTrue(fs.getPath("a", "b", "c").startsWith(fs.getPath("a", "b", "c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").startsWith(fs.getPath("a", "b", "c", "d")));
        Assert.assertFalse(fs.getPath("a", "b", "c").startsWith(fs.getPath("aa", "b", "c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").startsWith(fs.getPath("c")));

        Assert.assertTrue(fs.getPath("a", "b", "c").startsWith(fs.getPath("")));

        Assert.assertFalse(fs.getPath("a", "b", "c").startsWith(rootDir));
        Assert.assertTrue(rootDir.resolve("a").resolve("b").resolve("c").startsWith(rootDir));
        Assert.assertTrue(rootDir.resolve("a").resolve("b").resolve("c").startsWith(rootDir.resolve("a").resolve("b")));
        Assert.assertTrue(rootDir.resolve("a").resolve("b").resolve("c").startsWith(rootDir.resolve("a").resolve("b").resolve("c")));

        Assert.assertTrue(fs.getPath("123", "a", "b", "c").startsWith("123"));
        Assert.assertFalse(fs.getPath("123", "a", "b", "c").startsWith("1"));

        Assert.assertTrue(fs.getPath("a", "b", "c").endsWith(fs.getPath("")));
        Assert.assertTrue(fs.getPath("a", "b", "c").endsWith(fs.getPath("c")));
        Assert.assertTrue(fs.getPath("a", "b", "c").endsWith(fs.getPath("b", "c")));
        Assert.assertTrue(fs.getPath("a", "b", "c").endsWith(fs.getPath("a", "b", "c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").endsWith(fs.getPath("a", "b")));
        Assert.assertFalse(fs.getPath("a", "b", "c").endsWith(fs.getPath("z", "a", "b", "c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").endsWith(fs.getPath("0", "a", "b", "c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").endsWith(fs.getPath("a", "b", "cc")));

        Assert.assertTrue(rootDir.resolve("a").resolve("b").resolve("c").endsWith(rootDir.resolve("a").resolve("b").resolve("c")));
        Assert.assertFalse(fs.getPath("a", "b", "c").endsWith(rootDir.resolve("c")));

        Assert.assertTrue(fs.getPath("a", "b", "c", "123").endsWith(fs.getPath("123")));
        Assert.assertFalse(fs.getPath("a", "b", "c", "123").endsWith(fs.getPath("3")));
        Assert.assertTrue(fs.getPath("a", "b", "c", "123").endsWith("123"));
        Assert.assertFalse(fs.getPath("a", "b", "c", "123").endsWith("3"));

        {
            final List<Path> paths = new ArrayList<>();
            for (final Path path : fs.getPath("a", "b", "c", "d")) {
                paths.add(path);
            }
            Assert.assertEquals(Arrays.asList(fs.getPath("a"), fs.getPath("b"), fs.getPath("c"), fs.getPath("d")), paths);
        }

        Assert.assertEquals(1, fs.getPath("a").getNameCount());
        Assert.assertEquals(2, fs.getPath("a", "b").getNameCount());
        Assert.assertEquals(3, fs.getPath("a", "b", "c").getNameCount());
        Assert.assertEquals(fs.getPath("b"), fs.getPath("a", "b").getName(1));
        Assert.assertEquals(fs.getPath("b"), fs.getPath("a", "b", "c").getName(1));

        Assert.assertEquals(0, rootDir.getNameCount());
        Assert.assertEquals(2, rootDir.resolve("a").resolve("b").getNameCount());

        Assert.assertEquals(rootDir.resolve("a").resolve("b"), rootDir.resolve("a").resolve("b").resolve("c").getParent());
        Assert.assertEquals(rootDir.resolve("a"), rootDir.resolve("a").resolve("b").resolve("c").getParent().getParent());
        Assert.assertEquals(null, rootDir.resolve("a").resolve("b").resolve("c").getParent().getParent().getParent());

        Assert.assertEquals(fs.getPath("a"), rootDir.resolve("a").resolve("b").getName(0));
        Assert.assertEquals(fs.getPath("b"), rootDir.resolve("a").resolve("b").getName(1));

        Assert.assertEquals(fs.getPath("a", "b"), rootDir.relativize(rootDir.resolve("a").resolve("b")));
        Assert.assertEquals(fs.getPath("b"), rootDir.resolve("a").relativize(rootDir.resolve("a").resolve("b")));

        Assert.assertEquals(fs.getPath("a"), fs.getPath("a").resolve(""));

        Assert.assertEquals(fs.getPath("a", "b"), ((RemoteCachingPath) rootDir.resolve("a").resolve("b")).asRelativePath());
        Assert.assertEquals(fs.getPath("a", "b"), ((RemoteCachingPath) fs.getPath("a").resolve("b")).asRelativePath());

        Assert.assertTrue(Files.isSameFile(fs.getPath("a", "b", "c"), fs.getPath("a").resolve("b").resolve("c")));
        Assert.assertFalse(Files.isSameFile(fs.getPath("a", "b"), fs.getPath("a").resolve("b").resolve("c")));
        Assert.assertFalse(Files.isSameFile(fs.getPath("a", "b", "c"), fs.getPath("a").resolve("b")));
        Assert.assertFalse(Files.isSameFile(fs.getPath("a", "b", "c"), rootDir.resolve("a").resolve("b").resolve("c")));
        Assert.assertFalse(Files.isSameFile(fs.getPath("a", "b", "c"), null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedRelativization() throws IOException {
        final RemoteCachingPath rootDir = RemoteCachingPath.getRoot(testContext.getFs());
        rootDir.resolve("a").resolve("b").relativize(rootDir.resolve("a").resolve("c"));
    }

    @Test
    public void testPathMetaOps() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final File bDir = new File(storeDir, "b");
        Assert.assertTrue(bDir.mkdir());
        final File cDir = new File(bDir, "c");
        Assert.assertTrue(cDir.mkdir());
        writeToFile(new File(cDir, "bc1"), "bc1");
        writeToFile(new File(cDir, "bc2"), "bc2");
        writeToFile(new File(cDir, "bc3"), "bc3");

        final FileSystem fs = testContext.getFs();

        Assert.assertTrue(Files.exists(fs.getPath("a", "aa1")));
        Assert.assertTrue(Files.notExists(fs.getPath("a", "aa2")));

        Assert.assertTrue(Files.isDirectory(fs.getPath("a")));
        Assert.assertFalse(Files.isDirectory(fs.getPath("a", "aa1")));

        Assert.assertTrue(Files.isReadable(fs.getPath("a")));
        Assert.assertTrue(Files.isReadable(fs.getPath("a", "aa1")));
        Assert.assertFalse(Files.isReadable(fs.getPath("a", "aa2")));

        Assert.assertTrue(Files.isExecutable(fs.getPath("a")));
        Assert.assertFalse(Files.isExecutable(fs.getPath("a", "aa1")));

        Assert.assertFalse(Files.isWritable(fs.getPath("a")));
        Assert.assertFalse(Files.isWritable(fs.getPath("a", "aa1")));

        Assert.assertFalse(Files.isSymbolicLink(fs.getPath("a")));
        Assert.assertFalse(Files.isSymbolicLink(fs.getPath("a", "aa1")));

        Assert.assertFalse(Files.isHidden(fs.getPath("a")));
        Assert.assertFalse(Files.isHidden(fs.getPath("a", "aa1")));

        Assert.assertFalse(Files.isRegularFile(fs.getPath("a")));
        Assert.assertTrue(Files.isRegularFile(fs.getPath("a", "aa1")));

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

        final BasicFileAttributes bc3Attr = Files.readAttributes(fs.getPath("b", "c", "bc3"), BasicFileAttributes.class);
        Assert.assertFalse(bc3Attr.isDirectory());
        Assert.assertEquals(4, bc3Attr.size());

        Assert.assertEquals(Sets.newHashSet(
                fs.getPath("a"),
                fs.getPath("b")
                ),
                FluentIterable.from(Files.newDirectoryStream(fs.getPath(""))).toSet());

        Assert.assertEquals(
                Arrays.asList(fs.getPath("b", "c")),
                FluentIterable.from(Files.newDirectoryStream(fs.getPath("b"))).toList());

        Assert.assertEquals(Sets.newHashSet(
                fs.getPath("b", "c", "bc1"),
                fs.getPath("b", "c", "bc2"),
                fs.getPath("b", "c", "bc3")
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("b", "c"))).toSet());
    }

    @Test(expected = NotDirectoryException.class)
    public void testListDirOnFile() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        Assert.assertEquals(Sets.newHashSet(
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("a", "aa1"))).toSet());
    }

    @Test(expected = NoSuchFileException.class)
    public void testListDirOnMissingPath() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        Assert.assertEquals(Sets.newHashSet(
        ), FluentIterable.from(Files.newDirectoryStream(fs.getPath("a"))).toSet());
    }

    @Test
    public void testPathSerialization() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        final File path1File = new File(testContext.getTempRootDir(), "path1.dat");
        final Path path1 = RemoteCachingPath.getRoot(fs).resolve("a").resolve("b").resolve("c");
        com.indeed.util.io.Files.writeObjectToFileOrDie(path1, path1File.toString());
        final Path readPath1 = com.indeed.util.io.Files.readObjectFromFile(path1File.toString(), Path.class);
        Assert.assertEquals(path1, readPath1);
    }

    @Test
    public void testFileReplication() throws IOException {
        final File cacheDir = testContext.getCacheDir();
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final File bDir = new File(storeDir, "b");
        Assert.assertTrue(bDir.mkdir());
        final File cDir = new File(bDir, "c");
        Assert.assertTrue(cDir.mkdir());
        writeToFile(new File(cDir, "bc1"), "bc1");
        writeToFile(new File(cDir, "bc2"), "bc2");
        writeToFile(new File(cDir, "bc3"), "bc3");

        final FileSystem fs = testContext.getFs();

        // check that we can read data replicated from the store
        try (BufferedReader bufferedReader = Files.newBufferedReader(fs.getPath("/a", "aa1"), Charsets.UTF_8)) {
            Assert.assertEquals("this is a test", bufferedReader.readLine());
            Assert.assertEquals("here is another test", bufferedReader.readLine());
            Assert.assertNull(bufferedReader.readLine());
        }

        // check that we have the same information replicated to the local cache store
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(cacheDir.getPath(), "/a", "aa1"), Charsets.UTF_8)) {
            Assert.assertEquals("this is a test", bufferedReader.readLine());
            Assert.assertEquals("here is another test", bufferedReader.readLine());
            Assert.assertNull(bufferedReader.readLine());
        }

        Assert.assertEquals(new File(new File(cacheDir, "a"), "aa1"), fs.getPath("/a", "aa1").toFile());

        Assert.assertEquals("bc1\n", readFromPath(fs.getPath("/b", "c", "bc1")));
        Assert.assertEquals("bc2\n", readFromPath(fs.getPath("/b", "c", "bc2")));
        Assert.assertEquals("bc3\n", readFromPath(fs.getPath("/b", "c", "bc3")));
    }

    @Test(expected = IOException.class)
    public void testOpenDirectoryFailure() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final FileSystem fs = testContext.getFs();

        try (BufferedReader bufferedReader = Files.newBufferedReader(fs.getPath("/a"), Charsets.UTF_8)) {
            bufferedReader.readLine();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteFailure() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final FileSystem fs = testContext.getFs();
        Files.delete(fs.getPath("a", "aa1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteIfExistFailure() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final FileSystem fs = testContext.getFs();
        Files.deleteIfExists(fs.getPath("a", "aa2"));
    }

    @Test(expected = IOException.class)
    public void testOpenNonExistingFailure() throws IOException {
        final File storeDir = testContext.getLocalStoreDir();

        final File aDir = new File(storeDir, "a");
        Assert.assertTrue(aDir.mkdir());
        writeToFile(new File(aDir, "aa1"), "this is a test", "here is another test");

        final FileSystem fs = testContext.getFs();

        try (BufferedReader bufferedReader = Files.newBufferedReader(fs.getPath("/a", "non-existing"), Charsets.UTF_8)) {
            bufferedReader.readLine();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteToNewFileFailure() throws IOException {
        final FileSystem fs = testContext.getFs();

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(fs.getPath("new-file"), Charsets.UTF_8)) {
            bufferedWriter.write("a");
            bufferedWriter.flush();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateDirectoryFailure() throws IOException {
        final FileSystem fs = testContext.getFs();

        Files.createDirectory(fs.getPath("a"));
    }
}