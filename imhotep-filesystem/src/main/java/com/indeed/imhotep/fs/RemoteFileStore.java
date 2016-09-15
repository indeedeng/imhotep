package com.indeed.imhotep.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.List;
import java.util.Map;

/**
 * @author darren
 */

abstract class RemoteFileStore extends FileStore {
    @Override
    public String type() {
        return getClass().getName();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public long getTotalSpace() throws IOException {
        return 0;
    }

    @Override
    public long getUsableSpace() throws IOException {
        return 0;
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return 0;
    }

    abstract List<RemoteFileAttributes> listDir(RemoteCachingPath path) throws IOException;

    @Override
    public boolean supportsFileAttributeView(final Class<? extends FileAttributeView> type) {
        return (BasicFileAttributeView.class == type) || (RemoteCachingFileAttributeViews.Imhotep.class == type);
    }

    @Override
    public boolean supportsFileAttributeView(final String name) {
        return "basic".equals(name) || "imhotep".equals(name);
    }

    @Override
    public Object getAttribute(final String attribute) throws IOException {
        throw new UnsupportedOperationException("\'" + attribute + "\' not recognized");
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(final Class<V> type) {
        return null;
    }

    abstract RemoteFileAttributes getRemoteAttributes(RemoteCachingPath path) throws IOException;

    RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path, final boolean isFile) throws IOException {
        final RemoteFileAttributes result = getRemoteAttributes(path);

        if ((result != null) && (result.isFile == isFile)) {
            return result;
        } else {
            throw new NoSuchFileException(path + " not found");
        }
    }

    abstract void downloadFile(RemoteCachingPath srcPath, Path destPath) throws IOException;

    /**
     * open an input stream without caching
     * @param path the remote path
     * @param startOffset the offset bytes
     * @param length bytes you want to read
     * @return the input stream
     */
    abstract InputStream newInputStream(RemoteCachingPath path,
                                        long startOffset,
                                        long length) throws IOException;

    static class RemoteFileAttributes {
        private final RemoteCachingPath path;
        private final long size;
        // TODO: should this be isDirectory?
        private final boolean isFile;

        RemoteFileAttributes(final RemoteCachingPath path, final long size, final boolean isFile) {
            this.path = path;
            this.size = size;
            this.isFile = isFile;
        }

        public RemoteCachingPath getPath() {
            return path;
        }

        public long getSize() {
            return size;
        }

        public boolean isFile() {
            return isFile;
        }

        public boolean isDirectory() {
            return !isFile;
        }
    }

    public interface Factory {
        RemoteFileStore create(Map<String, ?> configuration);
    }
}
