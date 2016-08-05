package com.indeed.imhotep.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.List;
import java.util.Map;

/**
 * @author darren
 */

public abstract class RemoteFileStore extends FileStore {
    @Override
    public String type() {
        return getClass().getName();
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

    public abstract List<RemoteFileInfo> listDir(RemoteCachingPath path) throws IOException;

    public RemoteFileInfo readInfo(RemoteCachingPath path) throws IOException {
        return readInfo(path.toString());
    }

    @Override
    public boolean supportsFileAttributeView(final Class<? extends FileAttributeView> type) {
        return ImhotepFileAttributeView.class.isInstance(type);
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

    public RemoteFileInfo readInfo(RemoteCachingPath path, boolean isFile) throws IOException {
        return readInfo(path.toString(), isFile);
    }

    // TODO: why string
    public abstract RemoteFileInfo readInfo(String shardPath) throws IOException;

    public RemoteFileInfo readInfo(String shardPath, boolean isFile) throws IOException {
        final RemoteFileInfo result = readInfo(shardPath);

        if (result != null && result.isFile == isFile) {
            return result;
        } else {
            return null;
        }
    }

    public abstract void downloadFile(RemoteCachingPath srcPath, Path destPath) throws IOException;

    public abstract InputStream getInputStream(String path,
                                               long startOffset,
                                               long length) throws IOException;

    public static class RemoteFileInfo {
        private String path;
        private long size;
        // TODO: should this be isDirectory?
        private final boolean isFile;

        public RemoteFileInfo(String path, long size, boolean isFile) {
            this.path = path;
            this.size = size;
            this.isFile = isFile;
        }
    }

    public interface Builder {
        RemoteFileStore build(Map<String, String> configuration);
    }
}
