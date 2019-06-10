package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributesMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildIOExceptionFromResponse;
import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildImhotepKnownExceptionFromResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void testListShardDirRecursively() throws IOException {
        testContext.createDailyShard("dataset4", 1, false);
        final Path shardPath = testContext.getShardPaths("dataset4").get(0);

        final Path remoteDirPath = testContext.getRootPath().resolve(shardPath);
        // copy as the list from request is immutable
        final List<FileAttributesMessage> attrMessageListFromRequest = new ArrayList<>(listShardFileRecursively(remoteDirPath));
        Collections.sort(attrMessageListFromRequest, Comparator.comparing(FileAttributesMessage::getPath));

        final List<FileAttributesMessage> localAttrMessageList;
        try (final Stream<Path> fileStream = Files.walk(shardPath)) {
            localAttrMessageList = fileStream.filter(Files::isRegularFile).map(path -> {
                try {
                    final BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                    return FileAttributesMessage.newBuilder()
                            .setPath(shardPath.relativize(path).toString())
                            .setSize(attrs.isDirectory() ? -1 : attrs.size())
                            .setIsDirectory(attrs.isDirectory())
                            .build();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }).sorted(Comparator.comparing(FileAttributesMessage::getPath)).collect(Collectors.toList());
        }

        assertEquals(localAttrMessageList.size(), attrMessageListFromRequest.size());
        for (int i = 0; i < localAttrMessageList.size(); i++) {
            assertEquals(localAttrMessageList.get(i), attrMessageListFromRequest.get(i));
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

    private List<FileAttributesMessage> listShardFileRecursively(final Path path) throws IOException {
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_DIR_FILES_RECURSIVELY)
                .setShardFileUri(path.toUri().toString())
                .build();
        return handleRequest(newRequest, (response, is) -> response.getFilesAttributesList());
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
