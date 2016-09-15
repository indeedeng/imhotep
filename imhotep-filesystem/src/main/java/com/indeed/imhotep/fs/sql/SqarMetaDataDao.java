package com.indeed.imhotep.fs.sql;

import com.indeed.imhotep.fs.RemoteFileMetadata;

import javax.annotation.Nullable;
import java.nio.file.Path;

/**
 * @author kenh
 */

public interface SqarMetaDataDao {
    void cacheMetadata(final Path shardPath, final Iterable<RemoteFileMetadata> metadataList);
    Iterable<RemoteFileMetadata> listDirectory(final Path shardPath, final String dirname);
    @Nullable RemoteFileMetadata getFileMetadata(final Path shardPath, final String filename);
    boolean hasShard(final Path shardPath);
}
