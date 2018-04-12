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

package com.indeed.imhotep.shardmaster;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * @author kenh
 */

class ShardScanner implements Iterable<ShardDir> {
    private final RemoteCachingPath datasetDir;
    private final ShardFilter shardFilter;

    ShardScanner(final RemoteCachingPath datasetDir, final ShardFilter shardFilter) {
        this.datasetDir = datasetDir;
        this.shardFilter = shardFilter;
    }

    @Nonnull
    @Override
    public Iterator<ShardDir> iterator() {
        // hack to avoid an extra attribute lookup on each list entry
        final RemoteCachingFileSystemProvider fsProvider = (RemoteCachingFileSystemProvider) (((Path) datasetDir).getFileSystem().provider());

        try (DirectoryStream<RemoteCachingPath> remoteCachingPaths = fsProvider.newDirectoryStreamWithAttributes(datasetDir, DataSetScanner.ONLY_DIRS)) {
            return FluentIterable.from(remoteCachingPaths)
                    .filter(new Predicate<Path>() {
                        @Override
                        public boolean apply(final Path shardPath) {
                            final String dataset = shardPath.getParent().getFileName().toString();
                            final String shard = shardPath.getFileName().toString();
                            final String shardId = new ShardDir(shardPath).getId();

                            // invalid shards should be ignored
                            return ShardTimeUtils.isValidShardId(shardId) &&
                                    shardFilter.accept(dataset, shard);
                        }
                    })
                    .transform(new Function<Path, ShardDir>() {
                        @Override
                        public ShardDir apply(final Path path) {
                            return new ShardDir(path);
                        }
                    }).toList().iterator();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards from " + datasetDir, e);
        }
    }
}
