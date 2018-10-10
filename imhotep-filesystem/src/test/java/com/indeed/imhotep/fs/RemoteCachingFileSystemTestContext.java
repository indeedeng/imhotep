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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.StringInputStream;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemTestContext extends ExternalResource {
    private final TemporaryFolder tempDir = new TemporaryFolder();

    private static final Map<String, String> DEFAULT_CONFIG = ImmutableMap.<String, String>builder()
            .put("imhotep.fs.store.type", "local")
            .put("imhotep.fs.cache.size.gb", "1")
            .put("imhotep.fs.cache.block.size.bytes", "4096")
            .build();

    private final Map<String, String> configuration;

    private RemoteCachingFileSystem fs;
    private File cacheDir;
    private File localStoreDir;
    private TestS3Endpoint testS3Endpoint;

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

    public static class TestS3Endpoint {
        private final String key;
        private final String secret;
        private final String bucket;
        private final String prefix;
        private AmazonS3Client client;

        public TestS3Endpoint() throws IOException {
            this(UUID.randomUUID().toString());
        }

        TestS3Endpoint(final String prefix) throws IOException {
            final Properties s3Properties = new Properties();
            try (InputStream s3ConfigIS = ClassLoader.getSystemResourceAsStream("s3-test.properties")) {
                s3Properties.load(s3ConfigIS);
            }
            key = s3Properties.getProperty("imhotep.fs.filestore.s3.key");
            secret = s3Properties.getProperty("imhotep.fs.filestore.s3.secret");
            bucket = s3Properties.getProperty("imhotep.fs.filestore.s3.bucket");
            this.prefix = prefix;

            if ((!Strings.isNullOrEmpty(key)) && (!Strings.isNullOrEmpty(secret)) && (!Strings.isNullOrEmpty(bucket))) {
                client = createClient();
            }
        }

        public void putFile(final Path path, final String contents) {
            Preconditions.checkNotNull(client, "S3 client is not initialized");
            final StringBuilder pathBuilder = new StringBuilder(prefix);
            for (final Path el : path) {
                pathBuilder.append('/').append(el.getFileName());
            }
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setExpirationTime(DateTime.now().plusHours(1).toDate());
            try {
                client.putObject(bucket, pathBuilder.toString(), new StringInputStream(contents), metadata);
            } catch (final UnsupportedEncodingException e) {
                throw new IllegalStateException("Failed to create file with prefix " + prefix, e);
            }
        }

        private void initialize() {
            if (client != null) {
                try {
                    final ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setExpirationTime(DateTime.now().plusHours(1).toDate());
                    client.putObject(bucket, prefix + "/_DUMMY", new StringInputStream(""), metadata);
                } catch (final UnsupportedEncodingException e) {
                    throw new IllegalStateException("Failed to create file with prefix " + prefix, e);
                }
            }
        }

        private AmazonS3Client createClient() {
            final BasicAWSCredentials credentials = new BasicAWSCredentials(key, secret);
            return new AmazonS3Client(credentials);
        }
    }

    public void putS3File(final Path path, final String contents) {
        testS3Endpoint.putFile(path, contents);
    }

    public static Map<String, String> getConfigFor(final Map<String, String> baseConfig, final File sqarDbDir, final File cacheDir, final File localStoreDir, final URI hdfsStoreDir, final TestS3Endpoint testS3Endpoint) throws IOException {
        return ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("imhotep.fs.cache.root.uri", cacheDir.toURI().toString())

                // local
                .put("imhotep.fs.filestore.local.root.uri", localStoreDir.toURI().toString())

                // hdfs
                .put("imhotep.fs.filestore.hdfs.root.uri", hdfsStoreDir.toString())

                // s3
                .put("imhotep.fs.filestore.s3.prefix", testS3Endpoint.prefix)
                .put("imhotep.fs.filestore.s3.bucket", testS3Endpoint.bucket)
                .put("imhotep.fs.filestore.s3.key", testS3Endpoint.key)
                .put("imhotep.fs.filestore.s3.secret", testS3Endpoint.secret)

                .put("imhotep.fs.sqardb.file", new File(sqarDbDir, "db.data").toString())
                .put("imhotep.fs.sqar.metadata.cache.path", new File(sqarDbDir, "lsmtree").toString())
                .build();
    }

    public static Map<String, String> getConfig(final TemporaryFolder rootDir) throws IOException {
        final File localStoreDir = rootDir.newFolder("local-store");
        return getConfigFor(
                DEFAULT_CONFIG,
                rootDir.newFolder("sqardb"), rootDir.newFolder("cache"), localStoreDir, localStoreDir.toURI(), new TestS3Endpoint());
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

        testS3Endpoint = new TestS3Endpoint();
        testS3Endpoint.initialize();

        fs = (RemoteCachingFileSystem) FileSystems.newFileSystem(URI.create("imhtpfs://somehost/some/path"),
                ImmutableMap.<String, String>builder()
                        .putAll(getConfigFor(configuration, tempDir.newFolder("sqardb"), cacheDir, localStoreDir, localStoreDir.toURI(), testS3Endpoint))
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
