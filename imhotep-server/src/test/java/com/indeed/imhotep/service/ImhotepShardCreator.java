package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.util.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * @author kenh
 */

public interface ImhotepShardCreator {
    void create(final File shardsDir, final String dataset, final String shardId, final FlamdexReader flamdexReader) throws IOException;

    ImhotepShardCreator DEFAULT = new ImhotepShardCreator() {
        @Override
        public void create(final File shardsDir, final String dataset, final String shardId, final FlamdexReader flamdexReader) throws IOException {
            final Path shardDir = shardsDir.toPath().resolve(dataset).resolve(shardId);
            SimpleFlamdexWriter.writeFlamdex(flamdexReader, new SimpleFlamdexWriter(shardDir, flamdexReader.getNumDocs()));
        }
    };

    ImhotepShardCreator GZIP_ARCHIVE = new ImhotepShardCreator() {
        @Override
        public void create(final File shardsDir, final String dataset, final String shardId, final FlamdexReader flamdexReader) throws IOException {
            final Path shardDir = shardsDir.toPath().resolve(dataset).resolve(shardId + ".sqar");
            final Path shardTempDir = shardsDir.toPath().resolve(dataset).resolve(shardId + "-tmp");
            try {
                SimpleFlamdexWriter.writeFlamdex(flamdexReader, new SimpleFlamdexWriter(shardTempDir, flamdexReader.getNumDocs()));

                final SquallArchiveWriter archiveWriter = new SquallArchiveWriter(
                        new org.apache.hadoop.fs.Path(LocalFileSystem.DEFAULT_FS).getFileSystem(new Configuration()),
                        new org.apache.hadoop.fs.Path(shardDir.toString()),
                        true, SquallArchiveCompressor.GZIP);
                archiveWriter.batchAppendDirectory(shardTempDir.toFile());
                archiveWriter.commit();
            } finally {
                Files.delete(shardTempDir.toString());
            }
        }
    };
}
