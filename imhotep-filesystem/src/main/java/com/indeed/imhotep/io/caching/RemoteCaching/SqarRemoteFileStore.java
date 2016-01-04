package com.indeed.imhotep.io.caching.RemoteCaching;

import com.almworks.sqlite4java.SQLiteException;
import com.indeed.imhotep.archive.FileMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by darren on 12/11/15.
 */
public class SqarRemoteFileStore extends RemoteFileStore {
    private final SqarManager sqarManager;
    private final RemoteFileStore backingStore;

    public SqarRemoteFileStore(RemoteFileStore backingStore,
                               Map<String, String> options) throws SQLiteException {
        this.backingStore = backingStore;
        sqarManager = new SqarManager(options);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String type() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return type == BasicFileAttributeView.class;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return name.equals("basic");
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        if (type == null)
            throw new NullPointerException();
        return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        throw new UnsupportedOperationException("does not support the given attribute");
    }

    @Override
    public long getTotalSpace() throws IOException {
        return 0L;
    }

    @Override
    public long getUsableSpace() throws IOException {
        return 0L;
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return 0L;
    }

    @Override
    public ArrayList<RemoteFileInfo> listDir(RemoteCachingPath path) throws IOException {
        return sqarManager.readDir(path);
    }

    @Override
    public RemoteFileInfo readInfo(RemoteCachingPath path) throws IOException {
        final FileMetadata md = getMetadata(path);

        if (md == null)
            return null;
        return new RemoteFileInfo(path.toString(), md.getSize(), md.isFile());
    }

    @Override
    public RemoteFileInfo readInfo(String path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void downloadFile(RemoteCachingPath path, Path tmpPath) throws IOException {
        final String archivePath;
        final InputStream archiveIS;
        final FileMetadata md = getMetadata(path);

        if (md == null) {
            throw new FileNotFoundException("Cannot find shard for " + path);
        }

        archivePath = sqarManager.getFullArchivePath(path, md.getArchiveFilename());
        archiveIS = backingStore.getInputStream(archivePath,
                                                md.getStartOffset(),
                                                md.getCompressedSize());
        try {
            sqarManager.copyDecompressed(archiveIS, tmpPath.toFile(), md, path.toString());
        } catch (IOException e) {
            Files.delete(tmpPath);
            throw e;
        } finally {
            archiveIS.close();
        }
    }

    @Override
    public InputStream getInputStream(String path, long startOffset, long length) throws
                                                                                  IOException {
        throw new UnsupportedOperationException();
    }

    private FileMetadata getMetadata(RemoteCachingPath path) throws IOException {
        FileMetadata md = sqarManager.getPathInfo(path);
        if (md == null) {
            loadMetadataForSqar(path);
            md = sqarManager.getPathInfo(path);
        }
        return md;
    }

    private void loadMetadataForSqar(RemoteCachingPath path) throws IOException {
        final String metadataLoc = sqarManager.getMetadataLoc(path);
        try (InputStream metadataIS = backingStore.getInputStream(metadataLoc, 0, -1)) {
            sqarManager.cacheMetadata(path, metadataIS);
        }
    }

}
