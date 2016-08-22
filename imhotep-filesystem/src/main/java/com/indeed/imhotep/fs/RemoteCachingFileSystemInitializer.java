package com.indeed.imhotep.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemInitializer implements Supplier<FileSystem> {
    private static final Object FS_LOCK = new Object();

    private final String fileSystemConfigFile;

    @VisibleForTesting
    RemoteCachingFileSystemInitializer(final String fileSystemConfigFile) {
        this.fileSystemConfigFile = fileSystemConfigFile;
    }

    public RemoteCachingFileSystemInitializer() {
         this(System.getProperty("imhotep.fs.config.file"));
    }

    @Override
    public FileSystem get() {
        final RemoteCachingFileSystemProvider fileSystemProvider = new RemoteCachingFileSystemProvider();
        final URI uri = RemoteCachingFileSystemProvider.URI;
        if (fileSystemConfigFile != null) {
            synchronized (FS_LOCK) {
                final FileSystem fileSystem = fileSystemProvider.getFileSystem(uri);
                if (fileSystem == null) {
                    try {
                        try (InputStream inputStream = Files.newInputStream(new File(fileSystemConfigFile).toPath())) {
                            final Properties properties = new Properties();
                            properties.load(inputStream);
                            final Map<String, Object> configuration = new HashMap<>();
                            for (final String prop : properties.stringPropertyNames()) {
                                configuration.put(prop, properties.get(prop));
                            }
                            return fileSystemProvider.newFileSystem(uri, configuration);
                        }
                    } catch (final IOException e) {
                        throw new IllegalStateException("Failed to read imhotep fs configuration from " + fileSystemConfigFile, e);
                    }
                }
                return fileSystem;
            }
        } else {
            return fileSystemProvider.getFileSystem(uri);
        }
    }
}
