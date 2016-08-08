package com.indeed.imhotep.fs;

import com.indeed.imhotep.archive.FileMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author darren
 */
public class SqarRemoteFileStore extends RemoteFileStore {
    private final SqarManager sqarManager;
    private final RemoteFileStore backingStore;

    public SqarRemoteFileStore(final RemoteFileStore backingStore,
                               final Map<String, String> configuration) {
        this.backingStore = backingStore;
        sqarManager = new SqarManager(configuration);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
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

        if (md == null) {
            return null;
        }
        return new RemoteFileInfo(path.toString(), md.getSize(), md.isFile());
    }

    @Override
    public RemoteFileInfo readInfo(RemoteCachingPath path, boolean isFile) throws IOException {
        final RemoteFileInfo result = readInfo(path);

        if ((result != null) && (result.isFile() == isFile)) {
            return result;
        } else {
            return null;
        }
    }

    @Override
    public RemoteFileInfo readInfo(String path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void downloadFile(RemoteCachingPath srcPath, Path destPath) throws IOException {
        final String archivePath;
        final InputStream archiveIS;
        final FileMetadata md = getMetadata(srcPath);

        if (md == null) {
            throw new FileNotFoundException("Cannot find shard for " + srcPath);
        }
        if (!md.isFile()) {
            throw new NoSuchFileException(srcPath.toString() + " is not a file");
        }

        archivePath = sqarManager.getFullArchivePath(srcPath, md.getArchiveFilename());
        archiveIS = backingStore.getInputStream(archivePath,
                md.getStartOffset(),
                md.getCompressedSize());
        try {
            sqarManager.copyDecompressed(archiveIS, destPath, md, srcPath.toString());
        } catch (IOException e) {
            Files.delete(destPath);
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
        SqarManager.PathInfoResult pathInfoResult = sqarManager.getPathInfo(path);

        if (pathInfoResult == SqarManager.PathInfoResult.ARCHIVE_MISSING) {
            loadMetadataForSqar(path);
            pathInfoResult = sqarManager.getPathInfo(path);
        }
        if (pathInfoResult == SqarManager.PathInfoResult.FILE_MISSING) {
            return null;
        }

        return pathInfoResult.metadata;
    }

    private void loadMetadataForSqar(RemoteCachingPath path) throws IOException {
        final String metadataLoc = sqarManager.getMetadataLoc(path);
        try (InputStream metadataIS = backingStore.getInputStream(metadataLoc, 0, -1)) {
            sqarManager.cacheMetadata(path, metadataIS);
        }
    }

}
