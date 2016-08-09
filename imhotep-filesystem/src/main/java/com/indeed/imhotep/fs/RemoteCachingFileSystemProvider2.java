package com.indeed.imhotep.fs;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

/**
 * @author kenh
 */

public class RemoteCachingFileSystemProvider2 extends FileSystemProvider {
    private static final String URI_SCHEME = "rcfs";

    private static RemoteCachingFileSystem2 fileSystem;

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
        Preconditions.checkArgument(uri.getAuthority() != null, "URI authority must be present");
        Preconditions.checkArgument(uri.getPath() != null, "URI path must be present");
        Preconditions.checkArgument(uri.getQuery() == null, "URI query must be absent");
        Preconditions.checkArgument(uri.getFragment() == null, "URI fragment must be absent");
    }

    @Override
    public synchronized FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
        checkUri(uri);
        if (fileSystem != null) {
            throw new FileSystemAlreadyExistsException("Multiple file systems not supported");
        }
        fileSystem = new RemoteCachingFileSystem2(this, env);
        return fileSystem;
    }

    @Override
    public synchronized FileSystem getFileSystem(final URI uri) {
        checkUri(uri);
        if (fileSystem == null) {
            throw new FileSystemNotFoundException("File system not yet created");
        }
        return fileSystem;
    }

    @Override
    public Path getPath(final URI uri) {
        return getFileSystem(uri).getPath(uri.getPath());
    }

    @Override
    public SeekableByteChannel newByteChannel(final Path path, final Set<? extends OpenOption> options, final FileAttribute<?>... attrs) throws IOException {
        return null;
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir, final DirectoryStream.Filter<? super Path> filter) throws IOException {
        return null;
    }

    @Override
    public void createDirectory(final Path dir, final FileAttribute<?>... attrs) throws IOException {

    }

    @Override
    public void delete(final Path path) throws IOException {

    }

    @Override
    public void copy(final Path source, final Path target, final CopyOption... options) throws IOException {

    }

    @Override
    public void move(final Path source, final Path target, final CopyOption... options) throws IOException {

    }

    @Override
    public boolean isSameFile(final Path path, final Path path2) throws IOException {
        if (path == null) {
            return path2 == null;
        } else {
            return path.equals(path2);
        }
    }

    @Override
    public boolean isHidden(final Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(final Path path) throws IOException {
        return fileSystem.
    }

    @Override
    public void checkAccess(final Path path, final AccessMode... modes) throws IOException {

    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(final Path path, final Class<V> type, final LinkOption... options) {
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(final Path path, final Class<A> type, final LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public Map<String, Object> readAttributes(final Path path, final String attributes, final LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public void setAttribute(final Path path, final String attribute, final Object value, final LinkOption... options) throws IOException {

    }
}
