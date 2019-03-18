package com.indeed.imhotep;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * @author xweng
 * @author xweng
 */
public class P2PCachingTestContext implements Closeable {

    private static final String INDEX_NAME = "index20171231.20180301170838";
    private final TemporaryFolder tempDir;
    private Path rootPath;
    private FileSystem fs;
    private Path localStorePath;

    private ImhotepDaemonRunner imhotepDaemonRunner;
    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;

    P2PCachingTestContext() throws  IOException, TimeoutException, InterruptedException {
        tempDir = new TemporaryFolder();
        setUp();
    }

    @Override
    public void close() throws IOException {
        tearDown();
    }

    Path getRootPath() {
        return rootPath;
    }

    public FileSystem getFs() {
        return fs;
    }

    int getDaemonPort() {
        return imhotepDaemonRunner.getActualPort();
    }

    List<String> getIndexFileNames(final String indexSubDir) throws IOException {
        final Path wholePath = localStorePath.resolve(indexSubDir);
        final List<String> fileList = new ArrayList<>();
        final DirectoryStream<Path> stream = Files.newDirectoryStream(wholePath);
        for (final Path path : stream) {
            if (Files.isDirectory(path)) {
                continue;
            }
            fileList.add(path.getFileName().toString());
        }
        return fileList;
    }

    String getIndexSubDirectory(final String dataset) {
        return dataset + "/" + INDEX_NAME;
    }

    void createIndex(final String indexSubDir, final boolean isSqarFile) throws IOException {
        final Path wholePath = localStorePath.resolve(indexSubDir);
        if (!Files.exists(wholePath)) {
            tempDir.newFolder(wholePath.toString());
        }

        if (isSqarFile) {
            final String remotePath = wholePath.toString() + ".sqar";
            final File tempLocalArchieveFolder = tempDir.newFolder("local-archieve-folder");
            createFlamdexIndex(tempLocalArchieveFolder.toPath());
            createSqarFiles(tempLocalArchieveFolder, remotePath);
        } else {
            createFlamdexIndex(wholePath);
        }
    }

    private void setUp() throws IOException, TimeoutException, InterruptedException {
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

        fs = RemoteCachingFileSystemProvider.newFileSystem(tempConfigFile);
        rootPath = Iterables.getFirst(fs.getRootDirectories(), null);

        // setup imhotep runner
        tempDir.newFolder("local-store/temp-root-dir");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                rootPath,
                localStorePath.resolve("temp-root-dir"),
                ImhotepShardCreator.DEFAULT);
        imhotepDaemonRunner = clusterRunner.startDaemon();
    }

    private void tearDown() throws IOException {
        try {
            clusterRunner.stop();
        } finally {
            tempDir.delete();
        }
    }

    private void createSqarFiles(final File archiveDir, final String destDir) throws IOException {
        final SquallArchiveWriter writer = new SquallArchiveWriter(
                new org.apache.hadoop.fs.Path(LocalFileSystem.DEFAULT_FS).getFileSystem(new Configuration()),
                new org.apache.hadoop.fs.Path(destDir),
                true
        );
        writer.batchAppendDirectory(archiveDir);
        writer.commit();
    }

    private void createFlamdexIndex(final Path dir) throws IOException {
        final SimpleFlamdexDocWriter.Config config = new SimpleFlamdexDocWriter.Config().setDocBufferSize(999999999).setMergeFactor(999999999);
        try (final FlamdexDocWriter writer = new SimpleFlamdexDocWriter(dir, config)) {
            final FlamdexDocument doc0 = new FlamdexDocument();
            doc0.setIntField("if1", Longs.asList(0, 5, 99));
            doc0.setIntField("if2", Longs.asList(3, 7));
            doc0.setStringField("sf1", Arrays.asList("a", "b", "c"));
            doc0.setStringField("sf2", Arrays.asList("0", "-234", "bob"));
            writer.addDocument(doc0);

            final FlamdexDocument doc1 = new FlamdexDocument();
            doc1.setIntField("if2", Longs.asList(6, 7, 99));
            doc1.setStringField("sf1", Arrays.asList("b", "d", "f"));
            doc1.setStringField("sf2", Arrays.asList("a", "b", "bob"));
            writer.addDocument(doc1);

            final FlamdexDocument doc2 = new FlamdexDocument();
            doc2.setStringField("sf1", Arrays.asList("", "a", "aa"));
            writer.addDocument(doc2);

            final FlamdexDocument doc3 = new FlamdexDocument();
            doc3.setIntField("if1", Longs.asList(0, 10000));
            doc3.setIntField("if2", Longs.asList(9));
            writer.addDocument(doc3);
        }
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
