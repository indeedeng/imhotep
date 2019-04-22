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

package com.indeed.imhotep.fs;

import com.indeed.imhotep.service.MetricStatsEmitter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    abstract void downloadFile(RemoteCachingPath srcPath, Path destPath) throws IOException;

    /**
     * Return the cached path if the file store has
     */
    abstract Optional<Path> getCachedPath(RemoteCachingPath path) throws IOException;

    /**
     * Wrapper of getForOpen method in local file cache
     */
    abstract Optional<ScopedCacheFile> getForOpen(RemoteCachingPath path) throws IOException;

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

    protected static void reportFileDownload(final MetricStatsEmitter statsEmitter, long size) {
        // When getting an unbounded InputStream we don't know how big it is ahead of time
        // but it's only currently used for metadata.txt files which are usually small and get cached for a long time
        if (size >= 0) {
            statsEmitter.count("file.cache.newly.downloaded.size", size);
        }
        statsEmitter.count("file.cache.newly.downloaded.files", 1);
    }

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
        RemoteFileStore create(Map<String, ?> configuration, final MetricStatsEmitter statsEmitter);
    }
}
