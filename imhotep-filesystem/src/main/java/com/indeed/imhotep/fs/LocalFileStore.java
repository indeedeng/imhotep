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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author darren
 */
class LocalFileStore extends RemoteFileStore {
    private static final Logger LOGGER = Logger.getLogger(LocalFileStore.class);
    private final Path root;

    private LocalFileStore(final Path root) {
        this.root = root;
    }

    private LocalFileStore(final Map<String, ?> configuration) throws URISyntaxException {
        this(Paths.get(new URI((String) configuration.get("imhotep.fs.filestore.local.root.uri"))));
    }

    @Override
    public List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final Path localDirPath = getLocalPath(path);
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream( localDirPath)) {

            return FluentIterable.from(dirStream).transform(new Function<Path, RemoteFileAttributes>() {
                @Override
                public RemoteFileAttributes apply(final Path localPath) {
                    try {
                        final BasicFileAttributes attributes = Files.readAttributes(localPath, BasicFileAttributes.class);
                        return new RemoteFileAttributes(
                                RemoteCachingPath.resolve(path, localDirPath.relativize(localPath)),
                                attributes.size(),
                                attributes.isRegularFile());
                    } catch (final IOException e) {
                        throw new IllegalStateException("Failed to get attributes for " + localPath + " while listing " + path, e);
                    }
                }
            }).toList();
        }
    }

    @Override
    public String name() {
        return root.toString();
    }

    @Override
    public RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        final Path localPath = getLocalPath(path);
        final BasicFileAttributes attributes = Files.readAttributes(localPath, BasicFileAttributes.class);
        return new RemoteFileAttributes(path, attributes.size(), attributes.isRegularFile());
    }

    @Override
    Optional<Path> getCachedPath(final RemoteCachingPath path) throws IOException {
        return Optional.empty();
    }

    @Override
    Optional<LocalFileCache.ScopedCacheFile> getForOpen(final RemoteCachingPath path) {
        return Optional.empty();
    }

    @Override
    public void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final Path destParent = destPath.getParent();
        if (destParent != null) {
            Files.createDirectories(destParent);
        }
        Files.copy(getLocalPath(srcPath), destPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        final Path localPath = getLocalPath(path);
        final InputStream is = Files.newInputStream(localPath);
        final long skippedTo;
        try {
            skippedTo = is.skip(startOffset);
        } catch (final IOException e) {
            Closeables2.closeQuietly(is, LOGGER);
            throw new IOException("Failed to open " + path + " with offset " + startOffset, e);
        }

        if (skippedTo != startOffset) {
            throw new IOException("Could not move offset for path " + path + " by " + startOffset);
        }
        return is;
    }

    private Path getLocalPath(final RemoteCachingPath path) {
        return RemoteCachingPath.resolve(root, path.asRelativePath());
    }

    static class Factory implements RemoteFileStore.Factory {
        @Override
        public RemoteFileStore create(final Map<String, ?> configuration, final MetricStatsEmitter statsEmitter) {
            try {
                return new LocalFileStore(configuration); // not reporting stats
            } catch (final URISyntaxException e) {
                throw new IllegalArgumentException("Failed to initialize local file store", e);
            }
        }
    }
}
