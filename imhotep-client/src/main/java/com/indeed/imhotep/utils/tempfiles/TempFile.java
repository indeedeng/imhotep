package com.indeed.imhotep.utils.tempfiles;

import com.indeed.imhotep.scheduling.SilentCloseable;
import lombok.experimental.Delegate;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class TempFile {
    private static final Logger LOGGER = Logger.getLogger(TempFile.class);

    private final TempFileType<?> tempFileType;
    private final Path path;
    @Nullable
    private final StackTraceElement[] stackTraceElements;
    private final EventListener eventListener;

    private final Object lock = new Object();
    private boolean removed = false;
    private int refCount = 0;

    TempFile(final TempFileType<?> tempFileType, final Path path, @Nullable final StackTraceElement[] stackTraceElements, final EventListener eventListener) {
        this.tempFileType = tempFileType;
        this.path = path;
        this.stackTraceElements = stackTraceElements;
        this.eventListener = eventListener;
    }

    TempFileState getState() {
        return new TempFileState(tempFileType, path, stackTraceElements);
    }

    @Override
    protected void finalize() throws Throwable {
        // This means that I/O streams are also unreachable. So it's safe to delete file as well.
        synchronized (lock) {
            if (!removed) {
                eventListener.didNotRemoveTempFile(getState());
                try {
                    Files.deleteIfExists(path);
                } catch (final Exception e) {
                    LOGGER.error("Failed to delete temp file " + path, e);
                }
            }
        }
        super.finalize();
    }

    private void incRef() {
        synchronized (lock) {
            if (removed) {
                throw new IllegalStateException("Try to open a file handle for removed temp file");
            }
            ++refCount;
        }
    }

    private void decRef() {
        synchronized (lock) {
            --refCount;
        }
    }

    public void removeFile() throws IOException {
        synchronized (lock) {
            if (removed) {
                eventListener.removeTwice(getState());
            }
            if (refCount != 0) {
                eventListener.removeReferencedFile(getState());
            }
            removed = true;
        }
        Files.deleteIfExists(path);
    }

    public void removeFileStillReferenced() throws IOException {
        synchronized (lock) {
            if (removed) {
                eventListener.removeTwice(getState());
            }
            removed = true;
        }
        Files.deleteIfExists(path);
    }

    public SilentCloseable addReference() {
        incRef();
        return this::decRef;
    }

    public InputStream inputStream() throws FileNotFoundException {
        return new TrackedInputStream(new FileInputStream(path.toFile()));
    }

    public OutputStream outputStream() throws FileNotFoundException {
        return new TrackedOutputStream(new FileOutputStream(path.toFile()));
    }

    public BufferedInputStream bufferedInputStream() throws FileNotFoundException {
        return new BufferedInputStream(new TrackedInputStream(new FileInputStream(path.toFile())));
    }

    public BufferedOutputStream bufferedOutputStream() throws FileNotFoundException {
        return new BufferedOutputStream(new TrackedOutputStream(new FileOutputStream(path.toFile())));
    }

    public long getFileLength() throws IOException {
        return Files.size(path);
    }

    /**
     * If you open a file handle, you should call {@link TempFile#addReference()} to let {@link TempFile} track the usage.
     */
    public Path getPath() {
        return path;
    }

    /**
     * If you open a file handle, you should call {@link TempFile#addReference()} to let {@link TempFile} track the usage.
     */
    public File getFile() {
        return path.toFile();
    }

    private class TrackedInputStream extends InputStream {
        @Delegate(types = InputStream.class, excludes = Closeable.class)
        private final FileInputStream inner;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private TrackedInputStream(final FileInputStream inner) {
            incRef();
            this.inner = inner;
        }

        @Override
        protected void finalize() throws Throwable {
            if (!closed.get()) {
                eventListener.didNotCloseInputStream(getState());
                try {
                    close();
                } catch (final IOException e) {
                    LOGGER.error("Failed to close input stream for " + path + " on finalizer", e);
                }
            }
            super.finalize();
        }

        @Override
        public void close() throws IOException {
            if (!closed.getAndSet(true)) {
                try {
                    inner.close();
                } finally {
                    decRef();
                }
            }
        }
    }

    private class TrackedOutputStream extends OutputStream {
        @Delegate(types = OutputStream.class, excludes = Closeable.class)
        private final FileOutputStream inner;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private TrackedOutputStream(final FileOutputStream inner) {
            incRef();
            this.inner = inner;
        }

        @Override
        protected void finalize() throws Throwable {
            if (!closed.get()) {
                eventListener.didNotCloseOutputStream(getState());
                try {
                    close();
                } catch (final IOException e) {
                    LOGGER.error("Failed to close output stream for " + path + " on finalizer", e);
                }
            }
            super.finalize();
        }

        @Override
        public void close() throws IOException {
            if (!closed.getAndSet(true)) {
                try {
                    inner.close();
                } finally {
                    decRef();
                }
            }
        }
    }
}
