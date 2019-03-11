package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributeMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestShardFileRequests {
    // file system variables
    private static final Map<String, String> DEFAULT_CONFIG = ImmutableMap.<String, String>builder()
            .put("imhotep.fs.store.type", "local")
            .put("imhotep.fs.cache.size.gb", "1")
            .put("imhotep.fs.cache.block.size.bytes", "4096")
            .build();

    private static final String INDEX_NAME = "index20171231.20180301170838";

    private static final TemporaryFolder tempDir = new TemporaryFolder();
    private static Path rootPath;
    private static ImhotepDaemonRunner imhotepDaemonRunner;
    private static ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private static File localStoreDir;

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        // set up file system
        tempDir.create();

        localStoreDir = tempDir.newFolder("local-store");
        final Properties properties = new Properties();
        properties.putAll(
                getFileSystemConfigs(
                    DEFAULT_CONFIG,
                    tempDir.newFolder("sqardb"),
                    tempDir.newFolder("cache"),
                    localStoreDir,
                    localStoreDir.toURI())
        );

        final File tempConfigFile = tempDir.newFile("temp-filesystem-config.properties");
        properties.store(new FileOutputStream(tempConfigFile.getPath()), null);

        final FileSystem fs = RemoteCachingFileSystemProvider.newFileSystem(tempConfigFile);
        rootPath = Iterables.getFirst(fs.getRootDirectories(), null);
        if (rootPath == null) {
            fail("root is null");
        }

        // start daemons
        tempDir.newFolder("local-store/temp-root-dir");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                rootPath.toFile(),
                rootPath.resolve("temp-root-dir").toFile(),
                ImhotepShardCreator.DEFAULT);
        imhotepDaemonRunner = clusterRunner.startDaemon();
    }

    @AfterClass
    public static void tearDown() throws IOException  {
        try {
            clusterRunner.stop();
        } finally {
            tempDir.delete();
        }
    }

    @Test
    public void testGetShardFile() throws IOException {
        final String indexSubDirectory = initializeTest("dataset_1");
        internalTestGetShardFile(indexSubDirectory);
    }

    @Test
    public void testGetShardFileSqar() throws IOException {
        final String indexSubDirectory = getIndexSubDirectory("dataset_2");
        final File localArchiveDir = tempDir.newFolder("temp-local-archive-dir");
        // create local archive dir
        createFlamdexIndex(localArchiveDir.toPath());
        // create sqar file and store them in hdfs
        final String remoteIndexDir = localStoreDir.getPath() + "/" + indexSubDirectory + ".sqar";
        createSqarFiles(localArchiveDir, remoteIndexDir);

        internalTestGetShardFile(indexSubDirectory);
    }

    @Test
    public void testGetShardFileAttributes() throws IOException {
        final String indexSubDirectory = initializeTest("dataset_3");

        // file
        final Path remoteFilePath = rootPath.resolve(indexSubDirectory).resolve("fld-if2.intdocs");
        final FileAttributeMessage fileAttributes = getShardFileAttributes(remoteFilePath);
        assertNotNull(fileAttributes);
        assertFalse(fileAttributes.getIsDirectory());
        assertEquals(6, fileAttributes.getSize());

        // directory
        final Path remoteDirPath = rootPath.resolve(indexSubDirectory);
        final FileAttributeMessage dirAttributes = getShardFileAttributes(remoteDirPath);
        assertNotNull(dirAttributes);
        assertTrue(dirAttributes.getIsDirectory());
        assertEquals(4096, dirAttributes.getSize());
    }

    @Test
    public void testListShardFileAttributes() throws IOException {
        final String indexSubDirectory = initializeTest("dataset_4");
        // directory
        final Path remoteDirPath = rootPath.resolve(indexSubDirectory);
        final List<FileAttributeMessage> subFileAttributes = listShardFileAttributes(remoteDirPath);
        final Map<Path, FileAttributeMessage> pathToAttributesMap = Maps.newHashMap();
        subFileAttributes.forEach(attribute ->  pathToAttributesMap.put(
                Paths.get(URI.create(attribute.getPath())),
                attribute));

        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(remoteDirPath)) {
            dirStream.forEach(localPath -> {
                try {
                    final BasicFileAttributes attributes = Files.readAttributes(localPath, BasicFileAttributes.class);
                    final FileAttributeMessage message = pathToAttributesMap.getOrDefault(localPath, null);
                    assertNotNull(message);
                    assertEquals(attributes.isDirectory(), message.getIsDirectory());
                    assertEquals(attributes.size(), message.getSize());
                } catch (final IOException e) {
                    fail("IOException: " + e.getMessage());
                }
            });
        }
    }

    private void internalTestGetShardFile(final String indexSubDirectory) throws IOException {
        final List<String> filenames = ImmutableList.of("fld-if2.intdocs", "fld-sf1.strdocs", "metadata.txt");
        for (final String filename : filenames) {
            final Path fieldPath = rootPath.resolve(indexSubDirectory).resolve(filename);
            final File downloadedFile = File.createTempFile("temp-downloaded", "");
            downloadedFile.deleteOnExit();
            downloadShardFiles(fieldPath.toUri().toString(), downloadedFile);
            assertTrue(FileUtils.contentEquals(fieldPath.toFile(), downloadedFile));
        }
    }

    private FileAttributeMessage getShardFileAttributes(final Path path) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(path.toUri().toString())
                .build();
        return handleRequest(newRequest, (response, is) -> response.getFileAttributes());
    }

    private List<FileAttributeMessage> listShardFileAttributes(final Path path) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(path.toUri().toString())
                .build();
        return handleRequest(newRequest, (response, is) -> response.getSubFilesAttributesList());
    }

    private void downloadShardFiles(final String remoteFilePath, final File destFile) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFilePath(remoteFilePath)
                .build();
        handleRequest(newRequest, (response, is) -> {
            try (final OutputStream outputStream = new FileOutputStream(destFile)) {
                IOUtils.copy(ByteStreams.limit(is, response.getFileLength()), outputStream);
            }
            return true;
        });
    }

    private <R> R handleRequest(final ImhotepRequest request, final ThrowingFunction<ImhotepResponse, InputStream, R> function) throws IOException {
        final Socket socket = new Socket("localhost", imhotepDaemonRunner.getActualPort());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        try {
            ImhotepProtobufShipping.sendProtobuf(request, os);
            final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);

            if (imhotepResponse.getResponseCode() != ImhotepResponse.ResponseCode.OK) {
                fail("wrong response code");
            }
            return function.apply(imhotepResponse, is);
        } finally {
            socket.close();
        }
    }

    public interface ThrowingFunction<K, T, R> {
        R apply(K k, T t) throws IOException;
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
            final Map<String, String> baseConfig,
            final File sqarDbDir,
            final File cacheDir,
            final File localStoreDir,
            final URI hdfsStoreDir) {
        return ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("imhotep.fs.cache.root.uri", cacheDir.toURI().toString())
                .put("imhotep.fs.enabled", "true")
                // local
                .put("imhotep.fs.filestore.local.root.uri", localStoreDir.toURI().toString())
                // hdfs
                .put("imhotep.fs.filestore.hdfs.root.uri", hdfsStoreDir.toString())
                .put("imhotep.fs.sqar.metadata.cache.path", new File(sqarDbDir, "lsmtree").toString())
                .build();
    }

    private static String getIndexSubDirectory(final String dataset) {
        return dataset + "/" + INDEX_NAME;
    }

    private String initializeTest(final String dataset) throws IOException {
        final String indexSubDirectory = getIndexSubDirectory(dataset);
        tempDir.newFolder("local-store/" + indexSubDirectory);
        createFlamdexIndex(localStoreDir.toPath().resolve(indexSubDirectory));
        return indexSubDirectory;
    }
}
