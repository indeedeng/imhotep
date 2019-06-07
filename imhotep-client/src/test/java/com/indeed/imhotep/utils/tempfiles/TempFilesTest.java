package com.indeed.imhotep.utils.tempfiles;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TempFilesTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void tearDown() {
        assertEquals(0, temporaryFolder.getRoot().list().length);
    }

    private static void waitGC(final BooleanSupplier check) throws InterruptedException {
        for (int i = 0; i < 30; ++i) {
            // Please, please, please collect them.
            System.gc();
            System.gc();
            System.gc();
            System.runFinalization();
            Thread.sleep(100);
            if (check.getAsBoolean()) {
                return;
            }
        }
    }

    @Test
    public void testNormalOperation() throws IOException {
        final EventListener eventListener = createMock(EventListener.class);
        replay(eventListener);

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .setExpirationMillis(60_000)
                .setTakeStackTrace(true)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        try (final InputStream inputStream = tempFile.bufferedInputStream()) {
            final byte[] actual = new byte[bytes.length];
            ByteStreams.readFully(inputStream, actual);
            assertArrayEquals(bytes, actual);
        }

        final List<TempFileState> openedFileStates = tempFilesForTest.getAllOpenedStates();
        assertNotNull(openedFileStates);
        assertEquals(1, openedFileStates.size());
        final TempFileState state = openedFileStates.get(0);
        assertEquals(TempFilesForTest.Type.A, state.getTempFileType());
        assertNotNull(state.getStackTraceElements());

        tempFile.removeFile();
        verify(eventListener);
    }

    @Test
    public void testRemoveStillReferenced() throws IOException {
        final EventListener eventListener = createMock(EventListener.class);
        replay(eventListener);

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .setExpirationMillis(60_000)
                .setTakeStackTrace(true)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        try (final InputStream inputStream = tempFile.bufferedInputStream()) {
            tempFile.removeFileStillReferenced();
            final byte[] actual = new byte[bytes.length];
            ByteStreams.readFully(inputStream, actual);
            assertArrayEquals(bytes, actual);
        }

        final List<TempFileState> openedFileStates = tempFilesForTest.getAllOpenedStates();
        assertNotNull(openedFileStates);
        assertEquals(1, openedFileStates.size());
        final TempFileState state = openedFileStates.get(0);
        assertEquals(TempFilesForTest.Type.A, state.getTempFileType());
        assertNotNull(state.getStackTraceElements());

        verify(eventListener);
    }

    /**
     * This test relies on unguaranteed GC behavior.
     */
    @Test
    public void testLeakInputStream() throws IOException, InterruptedException {
        final CountingEventListener eventListener = new CountingEventListener();

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        {
            InputStream inputStream = tempFile.bufferedInputStream(); // Don't close
            final byte[] actual = new byte[bytes.length];
            ByteStreams.readFully(inputStream, actual);
            assertArrayEquals(bytes, actual);
            //noinspection UnusedAssignment
            inputStream = null; // To make the object eligible for GC
        }
        waitGC(() -> eventListener.didNotCloseInputStreamCount.get() > 0);
        tempFile.removeFile();
        assertEquals(1, eventListener.didNotCloseInputStreamCount.get());
        assertEquals(1, eventListener.getSum());
    }

    /**
     * This test relies on unguaranteed GC behavior.
     */
    @Test
    public void testLeakOutputStream() throws IOException, InterruptedException {
        final CountingEventListener eventListener = new CountingEventListener();

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        {
            OutputStream outputStream = tempFile.bufferedOutputStream(); // Don't close
            outputStream.write(bytes);
            //noinspection UnusedAssignment
            outputStream = null; // To make the object eligible for GC
        }
        waitGC(() -> eventListener.didNotCloseOutputStreamCount.get() > 0);
        assertEquals(1, eventListener.didNotCloseOutputStreamCount.get());
        assertEquals(1, eventListener.getSum());
        tempFile.removeFile();
    }

    /**
     * This test relies on unguaranteed GC behavior.
     */
    @Test
    public void testLeakFile() throws IOException, InterruptedException {
        final CountingEventListener eventListener = new CountingEventListener();

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .build();
        {
            TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
            final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
            try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
                outputStream.write(bytes);
            }
            try (final InputStream inputStream = tempFile.bufferedInputStream()) {
                final byte[] actual = new byte[bytes.length];
                ByteStreams.readFully(inputStream, actual);
                assertArrayEquals(bytes, actual);
            }
            //noinspection UnusedAssignment
            tempFile = null; // To make the object eligible for GC
        }
        waitGC(() -> eventListener.didNotRemoveTempFileCount.get() > 0);
        assertEquals(1, eventListener.didNotRemoveTempFileCount.get());
        assertEquals(1, eventListener.getSum());
    }

    @Test
    public void testRemoveTwice() throws IOException {
        final EventListener eventListener = createMock(EventListener.class);
        eventListener.removeTwice(anyObject());
        expectLastCall().once();
        replay(eventListener);

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        try (final InputStream inputStream = tempFile.bufferedInputStream()) {
            final byte[] actual = new byte[bytes.length];
            ByteStreams.readFully(inputStream, actual);
            assertArrayEquals(bytes, actual);
        }
        tempFile.removeFile();
        tempFile.removeFile();
        verify(eventListener);
    }

    @Test
    public void testRemoveReferenced() throws IOException {
        final CountingEventListener eventListener = new CountingEventListener();

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .build();
        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        final InputStream inputStream = tempFile.bufferedInputStream(); // Don't close this object
        final byte[] actual = new byte[bytes.length];
        ByteStreams.readFully(inputStream, actual);
        assertArrayEquals(bytes, actual);
        tempFile.removeFile();

        assertEquals(1, eventListener.removeReferencedFileCount.get());
        assertEquals(1, eventListener.getSum());
    }

    @Test
    public void testExpiration() throws IOException {
        final CountingEventListener eventListener = new CountingEventListener();
        final StoppedTicker ticker = new StoppedTicker();

        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .setEventListener(eventListener)
                .setExpirationMillis(10)
                .setTicker(ticker)
                .build();

        final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
        final byte[] bytes = "thisistest".getBytes(StandardCharsets.UTF_8);
        try (final OutputStream outputStream = tempFile.bufferedOutputStream()) {
            outputStream.write(bytes);
        }
        try (final InputStream inputStream = tempFile.bufferedInputStream()) {
            final byte[] actual = new byte[bytes.length];
            ByteStreams.readFully(inputStream, actual);
            assertArrayEquals(bytes, actual);
        }
        ticker.add(1, TimeUnit.SECONDS);

        tempFilesForTest.tryCleanupExpiredFiles();

        assertEquals(1, eventListener.expiredCount.get());
        assertEquals(1, eventListener.getSum());
        tempFile.removeFile();
    }

    @Test
    public void testFilePath() throws IOException {
        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .build();
        {
            final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A);
            assertEquals(temporaryFolder.getRoot().getPath(), tempFile.getFile().getParent());
            final String name = tempFile.getFile().getName();
            assertThat(name, matchesRegex("a\\.[^.]*\\.tmp"));
            tempFile.removeFile();
        }
        {
            final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.A, "extra");
            assertEquals(temporaryFolder.getRoot().getPath(), tempFile.getFile().getParent());
            final String name = tempFile.getFile().getName();
            assertThat(name, matchesRegex("a\\.extra\\.[^.]*\\.tmp"));
            tempFile.removeFile();
        }
        {
            final TempFile tempFile = tempFilesForTest.createTempFile(TempFilesForTest.Type.B, "extra");
            assertEquals(temporaryFolder.getRoot().getPath(), tempFile.getFile().getParent());
            final String name = tempFile.getFile().getName();
            assertThat(name, matchesRegex("b\\.extra\\.[^.]*\\.tmp"));
            tempFile.removeFile();
        }
    }

    @Test
    public void testCleanup() throws IOException {
        final TempFilesForTest tempFilesForTest = TempFilesForTest.builder()
                .setRoot(temporaryFolder.getRoot().toPath())
                .build();

        final List<String> goodNames = ImmutableList.of(
                "a.tmp",
                "a.something.tmp",
                "b.other.thing.tmp"
        );
        final List<String> badNames = ImmutableList.of(
                "c.should.be.leaked.tmp",
                "ashould.be.leaked.tmp",
                "b.should.be.leakedtmp"
        );
        for (final String name : goodNames) {
            temporaryFolder.newFile(name);
        }
        for (final String name : badNames) {
            temporaryFolder.newFile(name);
        }

        tempFilesForTest.tryCleanupTempDirectory();
        assertEquals(
                badNames.stream().sorted().collect(Collectors.toList()),
                Stream.of(temporaryFolder.getRoot().list()).sorted().collect(Collectors.toList())
        );
        for (final String name : badNames) {
            Files.deleteIfExists(temporaryFolder.getRoot().toPath().resolve(name));
        }
    }
}