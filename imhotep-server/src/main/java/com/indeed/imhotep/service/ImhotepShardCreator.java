/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
