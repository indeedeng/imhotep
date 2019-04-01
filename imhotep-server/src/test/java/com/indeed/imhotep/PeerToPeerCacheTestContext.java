package com.indeed.imhotep;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.joda.time.DateTime;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author xweng
 */
public class PeerToPeerCacheTestContext extends ExternalResource {
    DateTime DEFAULT_SHARD_START_DATE = new DateTime(2018, 1, 1, 0, 0);

    private final TemporaryFolder tempDir;
    private Path rootPath;
    private Path localStorePath;
    private List<ImhotepDaemonRunner> imhotepDaemonRunners;
    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;

    private final int daemonCount;

    PeerToPeerCacheTestContext() {
        this(1);
    }

    PeerToPeerCacheTestContext(final int daemonCount) {
        tempDir = new TemporaryFolder();
        this.daemonCount = daemonCount;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        setUp();
    }

    @Override
    protected void after() {
        tearDown();
        super.after();
    }

    private void setUp() throws IOException, TimeoutException, InterruptedException {
        imhotepDaemonRunners = new ArrayList<>();
        // set up file system
        tempDir.create();
        final File localStoreDir = tempDir.newFolder("local-store");
        localStorePath = localStoreDir.toPath();

        final Properties properties = new Properties();
        properties.putAll(
                getFileSystemConfigs(
                        tempDir.newFolder("sqardb"),
                        tempDir.newFolder("cache"),
                        tempDir.newFolder("p2pcache"),
                        localStoreDir,
                        localStoreDir.toURI())
        );

        final File tempConfigFile = tempDir.newFile("imhotep-daemon-test-filesystem-config.properties");
        properties.store(new FileOutputStream(tempConfigFile.getPath()), null);

        final FileSystem fs = RemoteCachingFileSystemProvider.newFileSystem(tempConfigFile);
        rootPath = Iterables.getFirst(fs.getRootDirectories(), null);

        // setup imhotep runner
        tempDir.newFolder("local-store/temp-root-dir");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                rootPath,
                localStorePath,
                localStorePath.resolve("temp-root-dir"),
                ImhotepShardCreator.DEFAULT);

        for (int i = 0; i < daemonCount; i++) {
            imhotepDaemonRunners.add(clusterRunner.startDaemon());
        }
    }

    private void tearDown() {
        try {
            new RemoteCachingFileSystemProvider().clearFileSystem();
            clusterRunner.stop();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            tempDir.delete();
        }
    }

    Path getRootPath() {
        return rootPath;
    }

    TemporaryFolder getTempDir() {
        return tempDir;
    }

    List<Host> getDaemonHosts() {
        return imhotepDaemonRunners.stream().map(runner -> new Host("localhost", runner.getActualPort())).collect(Collectors.toList());
    }

    List<Path> getShardPaths(final String dataset) throws IOException {
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(rootPath.resolve(dataset))) {
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(dirStream.iterator(), Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
        }
    }

    ShardMasterAndImhotepDaemonClusterRunner getClusterRunner() {
        return clusterRunner;
    }

    public void createDailyShard(final String dataset, final int duration, final boolean isArchive) throws IOException {
        final ImhotepShardCreator creator = isArchive ? ImhotepShardCreator.GZIP_ARCHIVE : ImhotepShardCreator.DEFAULT;
        final ShardMasterAndImhotepDaemonClusterRunner clusterRunnerForIndex = new ShardMasterAndImhotepDaemonClusterRunner(
                localStorePath,
                localStorePath,
                tempDir.newFolder("temp-dir").toPath(),
                creator);

        try {
            final DateTime date = DEFAULT_SHARD_START_DATE;
            for (int i = 0; i < duration; i++) {
                clusterRunnerForIndex.createDailyShard(dataset, date.plusDays(i), createReader(i));
            }
        } finally {
            clusterRunnerForIndex.stop();
        }
    }

    private FlamdexReader createReader(final int index) {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final Function<Integer, FlamdexDocument> create = param -> {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.setIntField("if1", param);
            doc.setIntField("shardId", index);
            doc.setStringField("sf1", "str"+index);
            return doc;
        };
        // common part
        for (int i = 0; i < 10; i++) {
            flamdex.addDocument(create.apply(i));
        }
        // unique part
        for (int i = index * 10; i < ((index + 1) * 10); i++) {
            flamdex.addDocument(create.apply(i));
        }
        return flamdex;
    }

    private static Map<String, String> getFileSystemConfigs (
            final File sqarDbDir,
            final File cacheDir,
            final File p2pCacheDir,
            final File localStoreDir,
            final URI hdfsStoreDir) {
        return ImmutableMap.<String, String>builder()
                // cache
                .put("imhotep.fs.cache.root.uri", cacheDir.toURI().toString())
                // p2pcache
                .put("imhotep.fs.p2p.cache.root.uri", p2pCacheDir.toURI().toString())
                .put("imhotep.fs.p2p.cache.enable", "true")
                .put("imhotep.fs.enabled", "true")
                // local
                .put("imhotep.fs.filestore.local.root.uri", localStoreDir.toURI().toString())
                // hdfs
                .put("imhotep.fs.filestore.hdfs.root.uri", hdfsStoreDir.toString())
                .put("imhotep.fs.sqar.metadata.cache.path", new File(sqarDbDir, "lsmtree").toString())
                .put("imhotep.fs.store.type", "local")
                .put("imhotep.fs.cache.size.gb", "1")
                .put("imhotep.fs.cache.block.size.bytes", "4096")
                .put("imhotep.fs.p2p.cache.size.gb", "1")
                .put("imhotep.fs.p2p.cache.block.size.bytes", "4096")
                .build();
    }
}