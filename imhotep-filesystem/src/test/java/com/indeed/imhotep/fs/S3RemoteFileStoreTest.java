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

package com.indeed.imhotep.fs;

import com.google.common.base.Charsets;
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
    public final RemoteCachingFileSystemTestContext testContext = new RemoteCachingFileSystemTestContext(ImmutableMap.of(
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
        ), NIOFileUtils.listDirectory(fs.getPath("b")));

        Assert.assertEquals(Sets.newHashSet(
                fs.getPath("b", "c", "bc1"),
                fs.getPath("b", "c", "bc2"),
                fs.getPath("b", "c", "bc3")
        ), NIOFileUtils.listDirectory(fs.getPath("b", "c")));
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
        ), NIOFileUtils.listDirectory(fs.getPath("a", "aa1")));
    }

    @Test(expected = NoSuchFileException.class)
    public void testListDirOnMissingPath() throws IOException {
        final RemoteCachingFileSystem fs = testContext.getFs();

        Assert.assertEquals(Sets.newHashSet(
        ), NIOFileUtils.listDirectory(fs.getPath("a")));
    }
}