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
import java.util.Map;
import java.util.Properties;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemInitializerTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

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
        final Map<String, String> config = RemoteCachingFileSystemTestContext.getConfigFor(
                RemoteCachingFileSystemTestContext.DEFAULT_CONFIG,
                tempDir.newFolder("db"), tempDir.newFolder("cache"), tempDir.newFolder("store"));
        final File fsProp = tempDir.newFile("fs.properties");
        mapToProperties(config, fsProp);

        Assert.assertNull(new RemoteCachingFileSystemInitializer().get());
        Assert.assertNull(new RemoteCachingFileSystemProvider().getFileSystem(RemoteCachingFileSystemProvider.URI));

        final RemoteCachingFileSystemInitializer initializer = new RemoteCachingFileSystemInitializer(fsProp.toString());
        final FileSystem fileSystem = initializer.get();
        Assert.assertNotNull(fileSystem);
        Assert.assertEquals(fileSystem, new RemoteCachingFileSystemProvider().getFileSystem(RemoteCachingFileSystemProvider.URI));

        Assert.assertEquals(fileSystem, initializer.get());
        Assert.assertEquals(fileSystem, new RemoteCachingFileSystemInitializer(fsProp.toString()).get());
        Assert.assertEquals(fileSystem, new RemoteCachingFileSystemInitializer().get());
    }
}