package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributeMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
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
    private static P2PCachingTestContext testContext;
    private static RemoteCachingPath rootPath;

    @BeforeClass
    public static void setUp() throws IOException, TimeoutException, InterruptedException {
        testContext = new P2PCachingTestContext();
        rootPath = (RemoteCachingPath) testContext.getRootPath();
    }

    @AfterClass
    public static void tearDown() throws IOException  {
        testContext.close();
    }

    @Test
    public void testGetShardFile() throws IOException {
        final String indexSubDirectory = testContext.getIndexSubDirectory("dataset_1");
        testContext.createIndex(indexSubDirectory, false);
        internalTestGetShardFile(indexSubDirectory);
    }

    @Test
    public void testGetShardFileSqar() throws IOException {
        final String indexSubDirectory = testContext.getIndexSubDirectory("dataset_2");
        testContext.createIndex(indexSubDirectory, true);
        internalTestGetShardFile(indexSubDirectory);
    }

    @Test
    public void testGetShardFileAttributes() throws IOException {
        final String indexSubDirectory = testContext.getIndexSubDirectory("dataset_3");
        testContext.createIndex(indexSubDirectory, false);

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
        final String indexSubDirectory = testContext.getIndexSubDirectory("dataset_4");
        testContext.createIndex(indexSubDirectory, false);

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
        final Socket socket = new Socket("localhost", testContext.getDaemonPort());
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
}
