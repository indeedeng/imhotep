package com.indeed.imhotep.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author darren
 */
public class RemoteCachingFileSystemProvider extends FileSystemProvider {
    private static final String URI_SCHEME = "rcfs";

    private static RemoteCachingFileSystem fileSystem;

    public RemoteCachingFileSystemProvider() {
    }

    protected static String getFSname(URI uri) {
        final String scheme = uri.getScheme();
        if ((scheme == null) || !URI_SCHEME.equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("URI scheme is not '" + URI_SCHEME + "'");
        }
        return uri.getHost();
    }

    static RemoteCachingPath toRCP(Path path) {
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

    @Override
    public synchronized RemoteCachingFileSystem newFileSystem(URI uri, Map<String, ?> env) throws
            IOException {
        final String name = getFSname(uri);
        if (fileSystem != null) {
            throw new FileSystemAlreadyExistsException();
        }

        try {
            this.fileSystem = new RemoteCachingFileSystem(this, name, (Map<String, String>) env);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        return this.fileSystem;
    }

    @Override
    public synchronized RemoteCachingFileSystem getFileSystem(URI uri) {
        if (this.fileSystem == null) {
            throw new FileSystemNotFoundException();
        }
        return this.fileSystem;
    }

    @Override
    public Path getPath(URI uri) {
        return getFileSystem(uri).getPath(uri.getPath());
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options,
                                              FileAttribute<?>... attrs) throws IOException {
        final RemoteCachingPath rcPath = toRCP(path);
        return fileSystem.newByteChannel(rcPath, options, attrs);
    }

    @Override
    public FileChannel newFileChannel(Path path,
                                      Set<? extends OpenOption> options,
                                      FileAttribute<?>... attrs) throws IOException {
        final RemoteCachingPath rcPath = toRCP(path);
        return fileSystem.newFileChannel(rcPath, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir,
                                                    final DirectoryStream.Filter<? super Path> filter) throws
            IOException {
        return new DirectoryStream<Path>() {
            public Iterator<Path> itr;
            public boolean isClosed;

            {
                // sanity check
                if (!fileSystem.isDirectory(toRCP(dir))) {
                    throw new NotDirectoryException(dir.toString());
                }
            }

            @Override
            public synchronized Iterator<Path> iterator() {
                if (isClosed) {
                    throw new ClosedDirectoryStreamException();
                }
                if (itr != null) {
                    throw new IllegalStateException("Iterator has already been returned");
                }

                try {
                    final RemoteCachingPath path = toRCP(dir);
                    itr = fileSystem.iteratorOf(path, filter);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return new Iterator<Path>() {

                    @Override
                    public boolean hasNext() {
                        if (isClosed) {
                            return false;
                        }
                        return itr.hasNext();
                    }

                    @Override
                    public synchronized Path next() {
                        if (isClosed) {
                            throw new NoSuchElementException();
                        }
                        return itr.next();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public synchronized void close() throws IOException {
                isClosed = true;
            }

        };
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        final RemoteCachingPath rcPath = toRCP(path);
        return fileSystem.newInputStream(rcPath);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        final RemoteCachingPath rcPath = toRCP(path);
        return fileSystem.newOutputStream(rcPath, options);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        final RemoteCachingPath rcPath = toRCP(dir);
        fileSystem.createDirectory(rcPath, attrs);
    }

    @Override
    public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws
            IOException {
        // TODO: complete me
    }

    @Override
    public void createLink(Path link, Path existing) throws IOException {
        // TODO: complete me
    }

    @Override
    public void delete(Path path) throws IOException {
        throw new IOException("File Deletion is not supported.");
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        if (path == null || path2 == null) {
            return false;
        }
        if (path.equals(path2)) {
            return true;
        }
        if (path.getFileSystem() != path2.getFileSystem()) {
            return false;
        }
        return path.normalize().equals(path2.normalize());
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return null;
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        final RemoteCachingPath rcPath = toRCP(path);
        if (fileSystem.lookupAttributes(rcPath) == null) {
            throw new FileNotFoundException(path.toString());
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path,
                                                                Class<V> type,
                                                                LinkOption... options) {
        if (type != BasicFileAttributeView.class && type != ImhotepFileAttributeView.class) {
            return null;
        }

        final RemoteCachingPath rcPath = toRCP(path);
        try {
            final ImhotepFileAttributes attrs;
            attrs = fileSystem.lookupAttributes(rcPath);
            if (attrs == null) {
                return null;
            } else {
                return (V) (new ImhotepFileAttributeView());
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path,
                                                            Class<A> type,
                                                            LinkOption... options) throws
            IOException {
        if (type != BasicFileAttributes.class && type != ImhotepFileAttributes.class) {
            return null;
        }

        final RemoteCachingPath rcPath = toRCP(path);
        final ImhotepFileAttributes attrs = fileSystem.lookupAttributes(rcPath);
        if (attrs == null) {
            throw new FileNotFoundException(path.toString());
        }
        return (A) attrs;
    }

    @Override
    public Map<String, Object> readAttributes(Path path,
                                              String attributes,
                                              LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public void setAttribute(Path path,
                             String attribute,
                             Object value,
                             LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }
}
