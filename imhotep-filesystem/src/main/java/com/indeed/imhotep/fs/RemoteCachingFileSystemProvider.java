package com.indeed.imhotep.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemProvider extends FileSystemProvider {
    private static final Logger LOGGER = Logger.getLogger(RemoteCachingFileSystemProvider.class);
    static final String URI_SCHEME = "imhtpfs";
    public static final URI URI = java.net.URI.create(RemoteCachingFileSystemProvider.URI_SCHEME + ":///");

    private static class FileSystemHolder {
        private static RemoteCachingFileSystem fileSystem;

        synchronized RemoteCachingFileSystem create(final RemoteCachingFileSystemProvider fileSystemProvider, final Map<String, ?> env) throws IOException {
            if (fileSystem != null) {
                throw new FileSystemAlreadyExistsException("Multiple file systems not supported");
            }
            fileSystem = new RemoteCachingFileSystem(fileSystemProvider, env);
            return fileSystem;
        }

        synchronized FileSystem create(final File fsConfigFile) throws IOException {
            if (fileSystem != null) {
                throw new FileSystemAlreadyExistsException("Multiple file systems not supported");
            } else {
                try {
                    try (InputStream inputStream = Files.newInputStream(fsConfigFile.toPath())) {
                        final Properties properties = new Properties();
                        properties.load(inputStream);
                        final Map<String, Object> configuration = new HashMap<>();
                        for (final String prop : properties.stringPropertyNames()) {
                            configuration.put(prop, properties.get(prop));
                        }
                        // we use the FileSystems api to avoid duplicate instantiation of the provider
                        return FileSystems.newFileSystem(URI, configuration);
                    }
                } catch (final IOException e) {
                    throw new IllegalStateException("Failed to read imhotep fs configuration from " + fsConfigFile, e);
                }
            }
        }

        synchronized RemoteCachingFileSystem get() {
            return fileSystem;
        }

        synchronized void clear() {
            fileSystem = null;
        }
    }

    private static final FileSystemHolder FILE_SYSTEM_HOLDER = new FileSystemHolder();

    static RemoteCachingPath toRemoteCachePath(final Path path) {
        if (path == null) {
            throw new NullPointerException();
        }
        if (!(path instanceof RemoteCachingPath)) {
            throw new ProviderMismatchException();
        }
        return (RemoteCachingPath) path;
    }

    @Override
    public String getScheme() {
        return URI_SCHEME;
    }

    private void checkUri(final URI uri) {
        Preconditions.checkArgument(URI_SCHEME.equals(uri.getScheme()), "URI scheme does not match");
        Preconditions.checkArgument(uri.getPath() != null, "URI path must be present");
        Preconditions.checkArgument(uri.getQuery() == null, "URI query must be absent");
        Preconditions.checkArgument(uri.getFragment() == null, "URI fragment must be absent");
    }

    @VisibleForTesting
    public static FileSystem newFileSystem(final File fsConfigFile) throws IOException {
        return FILE_SYSTEM_HOLDER.create(fsConfigFile);
    }

    public static FileSystem newFileSystem() throws IOException {
        final String fsFilePath = System.getProperty("imhotep.fs.config.file");
        if (fsFilePath != null) {
            return newFileSystem(new File(fsFilePath));
        } else {
            return null;
        }
    }

    @Override
    public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
        checkUri(uri);
        return FILE_SYSTEM_HOLDER.create(this, env);
    }

    @Override
    public FileSystem getFileSystem(final URI uri) {
        checkUri(uri);
        return FILE_SYSTEM_HOLDER.get();
    }

    @VisibleForTesting
    void clearFileSystem() {
        FILE_SYSTEM_HOLDER.clear();
    }

    @Override
    public Path getPath(final URI uri) {
        return getFileSystem(uri).getPath(uri.getPath());
    }

    private void checkOpenOptions(final OpenOption openOption) {
        if (openOption == StandardOpenOption.READ) {
            return;
        }
        throw new UnsupportedOperationException("Unsupported open option " + openOption);
    }

    @Override
    public SeekableByteChannel newByteChannel(final Path path, final Set<? extends OpenOption> options, final FileAttribute<?>... attrs) throws IOException {
        for (final OpenOption option : options) {
            checkOpenOptions(option);
        }
        return FILE_SYSTEM_HOLDER.get().newByteChannel(toRemoteCachePath(path));
    }

    /**
     * this is a read-only file system so output stream is unsupported
     */
    @Override
    public OutputStream newOutputStream(final Path path, final OpenOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * a loophole interface to allow for efficient filter without doing a seprate attribute lookup
     */
    public DirectoryStream<RemoteCachingPath> newDirectoryStreamWithAttributes(final Path dir, final DirectoryStream.Filter<Pair<? extends Path, ? extends BasicFileAttributes>> filter) throws IOException {
        final FluentIterable<RemoteCachingPath> filtered = FluentIterable.from(FILE_SYSTEM_HOLDER.get().listDirWithAttributes(toRemoteCachePath(dir)))
                .filter(new Predicate<RemoteFileStore.RemoteFileAttributes>() {
                    @Override
                    public boolean apply(final RemoteFileStore.RemoteFileAttributes attributes) {
                        try {
                            return filter.accept(Pair.of(attributes.getPath(), new ImhotepFileAttributes(attributes.getSize(), attributes.isDirectory())));
                        } catch (final IOException e) {
                            LOGGER.warn("Failed to apply directory stream filtering on " + attributes + ". It will be ignored", e);
                            return false;
                        }
                    }
                }).transform(new Function<RemoteFileStore.RemoteFileAttributes, RemoteCachingPath>() {
                    @Override
                    public RemoteCachingPath apply(final RemoteFileStore.RemoteFileAttributes fileAttributes) {
                        return fileAttributes.getPath();
                    }
                });

        return new DirectoryStream<RemoteCachingPath>() {
            private boolean closed = false;

            @Override
            public Iterator<RemoteCachingPath> iterator() {
                if (closed) {
                    throw new IllegalStateException("DirectoryStream already closed");
                }
                return filtered.iterator();
            }

            @Override
            public void close() throws IOException {
                closed = true;
            }
        };
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir, final DirectoryStream.Filter<? super Path> filter) throws IOException {
        final Iterable<? extends Path> filteredPaths = Iterables.filter(FILE_SYSTEM_HOLDER.get().listDir(toRemoteCachePath(dir)),
                new Predicate<RemoteCachingPath>() {
                    @Override
                    public boolean apply(final RemoteCachingPath path) {
                        try {
                            return filter.accept(path);
                        } catch (final IOException e) {
                            LOGGER.warn("Failed to apply directory stream filtering on " + path + ". It will be ignored", e);
                            return false;
                        }
                    }
                });

        return new DirectoryStream<Path>() {
            private boolean closed = false;

            @Override
            public Iterator<Path> iterator() {
                if (closed) {
                    throw new IllegalStateException("DirectoryStream already closed");
                }
                return (Iterator<Path>) filteredPaths.iterator();
            }

            @Override
            public void close() throws IOException {
                closed = true;
            }
        };
    }

    @Override
    public void createDirectory(final Path dir, final FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(final Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(final Path source, final Path target, final CopyOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void move(final Path source, final Path target, final CopyOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSameFile(final Path path, final Path path2) throws IOException {
        return Objects.equal(path, path2);
    }

    @Override
    public boolean isHidden(final Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(final Path path) throws IOException {
        // TODO: currently we only support a single configured file store per file system
        return Iterables.getFirst(FILE_SYSTEM_HOLDER.get().getFileStores(), null);
    }

    @Override
    public void checkAccess(final Path path, final AccessMode... modes) throws IOException {
        final RemoteCachingPath remotePath = toRemoteCachePath(path);
        final ImhotepFileAttributes attributes = remotePath.getFileSystem().getFileAttributes(remotePath);
        if (attributes == null) {
            throw new NoSuchFileException("No entity at " + path);
        }

        for (final AccessMode mode : modes) {
            switch (mode) {
                case READ:
                    break;
                case EXECUTE:
                    if (!attributes.isDirectory()) {
                        throw new AccessDeniedException("Execute not supported for " + path);
                    }
                    break;
                case WRITE:
                    throw new AccessDeniedException("Write not supported for " + path);
                default:
                    throw new IllegalArgumentException("Unexpected mode for entity " + path);
            }
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(final Path path, final Class<V> type, final LinkOption... options) {
        final RemoteCachingPath rcPath = toRemoteCachePath(path);
        if (type == RemoteCachingFileAttributeViews.Imhotep.class) {
            return (V) new RemoteCachingFileAttributeViews.Imhotep(rcPath);
        } else if (type == RemoteCachingFileAttributeViews.Basic.class) {
            return (V) new RemoteCachingFileAttributeViews.Basic(rcPath);
        }

        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(final Path path, final Class<A> type, final LinkOption... options) throws IOException {
        RemoteCachingFileAttributeViews.Basic fileAttributeView = null;
        if (type == ImhotepFileAttributes.class) {
            fileAttributeView = getFileAttributeView(path, RemoteCachingFileAttributeViews.Imhotep.class, options);
        } else if (type == BasicFileAttributes.class) {
            fileAttributeView = getFileAttributeView(path, RemoteCachingFileAttributeViews.Basic.class, options);
        }

        if (fileAttributeView == null) {
            throw new UnsupportedOperationException();
        }

        return (A) fileAttributeView.readAttributes();
    }

    @Override
    public Map<String, Object> readAttributes(final Path path, final String attributes, final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(final Path path, final String attribute, final Object value, final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }
}
