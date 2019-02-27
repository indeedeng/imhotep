package com.indeed.imhotep;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.service.ImhotepDaemonRunner;
import com.indeed.imhotep.service.ImhotepShardCreator;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author xweng
 */
public class TestGetShardFile {
    private ShardMasterAndImhotepDaemonClusterRunner clusterRunner;
    private Path storeDir;
    private Path tempDir;

    private ImhotepDaemonRunner imhotepDaemonRunner;
    final String dataset = "dataset";

    @Before
    public void setUp() throws IOException {
        storeDir = Files.createTempDirectory("temp-imhotep");
        tempDir = Files.createTempDirectory("temp-imhotep");
        clusterRunner = new ShardMasterAndImhotepDaemonClusterRunner(
                storeDir.toFile(),
                tempDir.toFile(),
                ImhotepShardCreator.DEFAULT);
    }

    @After
    public void tearDown() throws IOException  {
        clusterRunner.stop();
        FileUtils.deleteDirectory(tempDir.toFile());
        FileUtils.deleteDirectory(storeDir.toFile());
    }

    @Test
    public void testGetShardFile() throws IOException, TimeoutException, InterruptedException{
        writeFlamdexAndStartDaemon();

        final Path datasetPath = storeDir.resolve(dataset);
        final File[] subFiles = datasetPath.toFile().listFiles();
        if (subFiles.length == 0) {
            fail();
        }
        final String indexDirName = subFiles[0].getName();
        final List<String> filenames = ImmutableList.of("fld-if2.intdocs", "fld-sf1.strdocs", "metadata.txt");
        for (final String filename : filenames) {
            final Path fieldPath = datasetPath.resolve(indexDirName).resolve(filename);
            File downloadedFile = File.createTempFile("temp-downloaded", "");
            downloadShardFiles(fieldPath.toString(), downloadedFile);
            assertTrue(FileUtils.contentEquals(fieldPath.toFile(), downloadedFile));
            downloadedFile.delete();
            downloadedFile.length();
        }
    }

    private void downloadShardFiles(final String remoteFilePath, final File destFile) throws IOException {
        final Socket socket = new Socket("localhost", imhotepDaemonRunner.getActualPort());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFilePath(remoteFilePath)
                .build();
        try {
            ImhotepProtobufShipping.sendProtobuf(newRequest, os);
            final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);

            if (imhotepResponse.getResponseCode() != ImhotepResponse.ResponseCode.OK) {
                fail("wrong response code");
            }

            try (final OutputStream outputStream = new FileOutputStream(destFile)) {
                IOUtils.copy(ByteStreams.limit(is, imhotepResponse.getFileLength()), outputStream);
            }
        } catch (final IOException e) {
            fail();
        } finally {
            Closeables2.closeQuietly(socket, null);
        }
    }

    public void writeFlamdexAndStartDaemon() throws IOException, TimeoutException, InterruptedException {
        final DateTime date = new DateTime(2018, 1, 1, 0, 0);
        clusterRunner.createDailyShard(dataset, date, createFlamdexIndex());
        imhotepDaemonRunner = clusterRunner.startDaemon();
    }

    private FlamdexReader createFlamdexIndex() throws IOException {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final FlamdexDocument doc0 = new FlamdexDocument();
        doc0.setIntField("if1", Longs.asList(0, 5, 99));
        doc0.setIntField("if2", Longs.asList(3, 7));
        doc0.setStringField("sf1", Arrays.asList("a", "b", "c"));
        doc0.setStringField("sf2", Arrays.asList("0", "-234", "bob"));
        flamdex.addDocument(doc0);

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.setIntField("if2", Longs.asList(6, 7, 99));
        doc1.setStringField("sf1", Arrays.asList("b", "d", "f"));
        doc1.setStringField("sf2", Arrays.asList("a", "b", "bob"));
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.setStringField("sf1", Arrays.asList("", "a", "aa"));
        flamdex.addDocument(doc2);

        final FlamdexDocument doc3 = new FlamdexDocument();
        doc3.setIntField("if1", Longs.asList(0, 10000));
        doc3.setIntField("if2", Longs.asList(9));
        flamdex.addDocument(doc3);

        return flamdex;
    }
}
