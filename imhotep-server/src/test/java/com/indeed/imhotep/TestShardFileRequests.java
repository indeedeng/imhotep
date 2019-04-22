package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributeMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildIOExceptionFromResponse;
import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildImhotepKnownExceptionFromResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestShardFileRequests {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public PeerToPeerCacheTestContext testContext = new PeerToPeerCacheTestContext();

    @Test
    public void testGetShardFile() throws IOException {
        testContext.createDailyShard("dataset1", 1, false);
        final Path shardPath = testContext.getShardPaths("dataset1").get(0);
        internalTestGetShardFile(shardPath, ImmutableList.of("fld-if1.intdocs", "fld-sf1.strdocs", "metadata.txt"));
    }

    @Test
    public void testFailToGetShardFile() throws IOException {
        thrown.expect(NoSuchFileException.class);

        testContext.createDailyShard("dataset11", 1, false);
        final Path shardPath = testContext.getShardPaths("dataset11").get(0);
        internalTestGetShardFile(shardPath, ImmutableList.of("fld-if3.intdocs"));
    }

    @Test
    public void testGetShardFileSqar() throws IOException {
        testContext.createDailyShard("dataset2", 1, true);
        final Path shardPath = testContext.getShardPaths("dataset2").get(0);
        internalTestGetShardFile(shardPath, ImmutableList.of("fld-if1.intdocs", "fld-sf1.strdocs", "metadata.txt"));
    }

    @Test
    public void testFailToGetShardFileSqar() throws IOException {
        thrown.expect(NoSuchFileException.class);

        testContext.createDailyShard("dataset21", 1, true);
        final Path shardPath = testContext.getShardPaths("dataset21").get(0);
        internalTestGetShardFile(shardPath, ImmutableList.of("fld-if3.intdocs", "fld-sf3.strdocs", "metadata.json"));
    }


    @Test
    public void testGetShardFileAttributes() throws IOException {
        testContext.createDailyShard("dataset3", 1, false);
        final Path shardPath = testContext.getShardPaths("dataset3").get(0);

        // file
        final Path remoteFilePath = shardPath.resolve("fld-if1.intdocs");
        final FileAttributeMessage fileAttributes = getShardFileAttributes(remoteFilePath);
        assertNotNull(fileAttributes);
        assertFalse(fileAttributes.getIsDirectory());
        assertEquals(20, fileAttributes.getSize());

        // directory
        final FileAttributeMessage dirAttributes = getShardFileAttributes(shardPath);
        assertNotNull(dirAttributes);
        assertTrue(dirAttributes.getIsDirectory());
    }

    @Test
    public void testListShardFileAttributes() throws IOException {
        testContext.createDailyShard("dataset4", 1, false);
        final Path shardPath = testContext.getShardPaths("dataset4").get(0);

        final Path remoteDirPath = testContext.getRootPath().resolve(shardPath);
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

    private void internalTestGetShardFile(final Path datasetIndexDir, final List<String> fileNames) throws IOException {
        for (final String filename : fileNames) {
            final Path fieldPath = datasetIndexDir.resolve(filename);
            final File downloadedFile = testContext.getTempDir().newFile("temp-downloaded" + UUID.randomUUID());
            downloadShardFiles(fieldPath.toUri().toString(), downloadedFile);
            assertTrue(FileUtils.contentEquals(fieldPath.toFile(), downloadedFile));
        }
    }

    private FileAttributeMessage getShardFileAttributes(final Path path) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE_ATTRIBUTES)
                .setShardFileUri(path.toUri().toString())
                .build();
        return handleRequest(newRequest, (response, is) -> response.getFileAttributes());
    }

    private List<FileAttributeMessage> listShardFileAttributes(final Path path) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_FILE_ATTRIBUTES)
                .setShardFileUri(path.toUri().toString())
                .build();
        return handleRequest(newRequest, (response, is) -> response.getSubFilesAttributesList());
    }

    private void downloadShardFiles(final String remoteFileUri, final File destFile) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFileUri(remoteFileUri)
                .build();
        handleRequest(newRequest, (response, is) -> {
            try (final OutputStream outputStream = new FileOutputStream(destFile)) {
                IOUtils.copy(ByteStreams.limit(is, response.getFileLength()), outputStream);
            }
            return true;
        });
    }

    private <R> R handleRequest(final ImhotepRequest request, final ThrowingFunction<ImhotepResponse, InputStream, R> function) throws IOException {
        final Host host = testContext.getDaemonHosts().get(0);
        final Socket socket = new Socket(host.getHostname(), host.getPort());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        try {
            ImhotepProtobufShipping.sendProtobuf(request, os);
            final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);

            if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
                throw buildImhotepKnownExceptionFromResponse(imhotepResponse, host.hostname, host.getPort(), null);
            }
            if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildIOExceptionFromResponse(imhotepResponse, host.getHostname(), host.getPort(), null);
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
