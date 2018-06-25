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

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.util.core.Pair;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;

/**
 * @author kenh
 */

class DataSetScanner implements Iterable<RemoteCachingPath> {
    private final Path datasetsDir;

    DataSetScanner(final Path datasetsDir) {
        this.datasetsDir = datasetsDir;
    }

    @Nonnull
    @Override
    public Iterator<RemoteCachingPath> iterator() {
        // hack to avoid an extra attribute lookup on each list entry
        final RemoteCachingFileSystemProvider fsProvider =  (RemoteCachingFileSystemProvider) (((Path) datasetsDir).getFileSystem().provider());

        try (DirectoryStream<RemoteCachingPath> remoteCachingPaths = fsProvider.newDirectoryStreamWithAttributes(datasetsDir, ONLY_DIRS)) {
            return remoteCachingPaths.iterator();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get datasets from " + datasetsDir, e);
        }
    }

    static final DirectoryStream.Filter<Pair<? extends Path, ? extends BasicFileAttributes>> ONLY_DIRS = new DirectoryStream.Filter<Pair<? extends Path, ? extends BasicFileAttributes>>() {
        @Override
        public boolean accept(final Pair<? extends Path, ? extends BasicFileAttributes> entry) throws IOException {
            return entry.getSecond().isDirectory();
        }
    };
}
