package com.indeed.imhotep.fs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemInitializerTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private void mapToProperties(final Map<String, String> config, final File target) throws IOException {
        final Properties properties = new Properties();
        for (final Map.Entry<String, String> entry : config.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        try (FileOutputStream os = new FileOutputStream(target)) {
            properties.store(os, "");
        }
    }

    @After
    public void tearDown() {
        new RemoteCachingFileSystemProvider().clearFileSystem();
    }

    @Test
    public void testIt() throws IOException, URISyntaxException {
        final Map<String, String> config = RemoteCachingFileSystemTestContext.getConfig(tempDir);
        final File fsProp = tempDir.newFile("fs.properties");
        mapToProperties(config, fsProp);

        Assert.assertNull(new RemoteCachingFileSystemProvider().getFileSystem(RemoteCachingFileSystemProvider.URI));

        final FileSystem fileSystem = RemoteCachingFileSystemProvider.newFileSystem(fsProp);
        Assert.assertEquals(fileSystem, Paths.get(RemoteCachingFileSystemProvider.URI).getFileSystem());
        Assert.assertEquals(fileSystem, new RemoteCachingFileSystemProvider().getFileSystem(RemoteCachingFileSystemProvider.URI));
    }

    @Test(expected = FileSystemAlreadyExistsException.class)
    public void testDuplicateInitilization() throws IOException, URISyntaxException {
        final Map<String, String> config = RemoteCachingFileSystemTestContext.getConfig(tempDir);
        final File fsProp = tempDir.newFile("fs.properties");
        mapToProperties(config, fsProp);

        Assert.assertNull(new RemoteCachingFileSystemProvider().getFileSystem(RemoteCachingFileSystemProvider.URI));

        final FileSystem fileSystem = RemoteCachingFileSystemProvider.newFileSystem(fsProp);
        Assert.assertEquals(fileSystem, Paths.get(RemoteCachingFileSystemProvider.URI).getFileSystem());
        RemoteCachingFileSystemProvider.newFileSystem(fsProp);
    }
}