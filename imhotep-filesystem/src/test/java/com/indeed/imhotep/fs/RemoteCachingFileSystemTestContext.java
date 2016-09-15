package com.indeed.imhotep.fs;

import com.google.common.collect.ImmutableMap;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemTestContext extends ExternalResource {
    private final TemporaryFolder tempDir = new TemporaryFolder();

    private static final Map<String, String> DEFAULT_CONFIG = ImmutableMap.<String, String>builder()
            .put("imhotep.fs.store.type", "local")
            .put("imhotep.fs.cache.size.gb", "1")
            .build();

    private final Map<String, String> configuration;

    private RemoteCachingFileSystem fs;
    private File cacheDir;
    private File localStoreDir;

    public RemoteCachingFileSystemTestContext(final Map<String, String> configuration) {
        final Map<String, String> merged = new HashMap<>(DEFAULT_CONFIG);
        merged.putAll(configuration);
        this.configuration = merged;
    }

    public RemoteCachingFileSystemTestContext() {
        configuration = DEFAULT_CONFIG;
    }

    public RemoteCachingFileSystemTestContext(final File localStoreDir, final Map<String, String> configuration) {
        this(configuration);
        this.localStoreDir = localStoreDir;
    }

    public RemoteCachingFileSystemTestContext(final File localStoreDir) {
        this(DEFAULT_CONFIG);
        this.localStoreDir = localStoreDir;
    }

    public RemoteCachingFileSystem getFs() {
        return fs;
    }

    public File getTempRootDir() {
        return tempDir.getRoot();
    }

    public File getCacheDir() {
        return cacheDir;
    }

    public File getLocalStoreDir() {
        return localStoreDir;
    }

    public static Map<String, String> getConfigFor(final Map<String, String> baseConfig, final File sqarDbDir, final File cacheDir, final File localStoreDir, final URI hdfsStoreDir) throws IOException {
        final Properties s3Properties = new Properties();
        try (InputStream s3ConfigIS = ClassLoader.getSystemResourceAsStream("s3-test.properties")) {
            s3Properties.load(s3ConfigIS);
        }

        return ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("imhotep.fs.cache.root.uri", cacheDir.toURI().toString())

                // local
                .put("imhotep.fs.filestore.local.root.uri", localStoreDir.toURI().toString())

                // hdfs
                .put("imhotep.fs.filestore.hdfs.root.uri", hdfsStoreDir.toString())

                // s3
                .put("imhotep.fs.filestore.s3.prefix", "")
                .put("imhotep.fs.filestore.s3.bucket", s3Properties.getProperty("imhotep.fs.filestore.s3.bucket"))
                .put("imhotep.fs.filestore.s3.key", s3Properties.getProperty("imhotep.fs.filestore.s3.key"))
                .put("imhotep.fs.filestore.s3.secret", s3Properties.getProperty("imhotep.fs.filestore.s3.secret"))

                .put("imhotep.fs.sqardb.file", new File(sqarDbDir, "db.data").toString())
                .build();
    }

    public static Map<String, String> getConfig(final TemporaryFolder rootDir) throws IOException {
        final File localStoreDir = rootDir.newFolder("local-store");
        return getConfigFor(
                DEFAULT_CONFIG,
                rootDir.newFolder("sqardb"), rootDir.newFolder("cache"), localStoreDir, localStoreDir.toURI());
    }

    private static void mapToProperties(final Map<String, String> config, final File target) throws IOException {
        final Properties properties = new Properties();
        for (final Map.Entry<String, String> entry : config.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        try (FileOutputStream os = new FileOutputStream(target)) {
            properties.store(os, "");
        }
    }

    public static File getConfigAsFile(final TemporaryFolder rootDir) throws IOException {
        final File configFile = rootDir.newFile("fs.properties");
        mapToProperties(getConfig(rootDir), configFile);
        return configFile;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        tempDir.create();

        cacheDir = tempDir.newFolder("cache");
        if (localStoreDir == null) {
            localStoreDir = tempDir.newFolder("local-store");
        }

        fs = (RemoteCachingFileSystem) FileSystems.newFileSystem(URI.create("imhtpfs://somehost/some/path"),
                ImmutableMap.<String, String>builder()
                        .putAll(getConfigFor(configuration, tempDir.newFolder("sqardb"), cacheDir, localStoreDir, localStoreDir.toURI()))
                        .build()
        );
    }

    @Override
    protected void after() {
        ((RemoteCachingFileSystemProvider) fs.provider()).clearFileSystem();
        tempDir.delete();
        super.after();
    }
}
