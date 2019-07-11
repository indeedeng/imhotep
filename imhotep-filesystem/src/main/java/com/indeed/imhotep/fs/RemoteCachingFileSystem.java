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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;


/**
 * @author kenh
 */

public class RemoteCachingFileSystem extends FileSystem {
    private static final Logger LOGGER = Logger.getLogger(RemoteCachingFileSystem.class);
    private final RemoteCachingFileSystemProvider provider;
    private final SqarRemoteFileStore fileStore;
    private final PeerToPeerCacheFileStore peerToPeerCacheFileStore;
    private final LocalFileCache fileCache;

    RemoteCachingFileSystem(final RemoteCachingFileSystemProvider provider, final Map<String, ?> configuration, final MetricStatsEmitter statsEmitter) throws IOException {
        this.provider = provider;

        final RemoteFileStore backingFileStore = RemoteFileStoreType.fromName((String) configuration.get("imhotep.fs.store.type"))
                .getFactory().create(configuration, statsEmitter);

        final boolean enableP2PCaching = Boolean.parseBoolean((String) configuration.get("imhotep.fs.p2p.cache.enable"));
        peerToPeerCacheFileStore = enableP2PCaching ? new PeerToPeerCacheFileStore(this, configuration, statsEmitter) : null;
        fileStore = new SqarRemoteFileStore(backingFileStore, configuration);

        final URI cacheRootUri;
        try {
            cacheRootUri = new URI((String) configuration.get("imhotep.fs.cache.root.uri"));
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Could not parse cache root URI", e);
        }

        fileCache = new LocalFileCache(
                this,
                Paths.get(cacheRootUri),
                Long.parseLong((String) configuration.get("imhotep.fs.cache.size.gb")) * 1024 * 1024 * 1024,
                Integer.parseInt((String) configuration.get("imhotep.fs.cache.block.size.bytes")),
                new LocalFileCache.CacheFileLoader() {
                    @Override
                    public void load(final RemoteCachingPath src, final Path dest) throws IOException {
                        fileStore.downloadFile(src, dest);
                    }
                },
                statsEmitter,
                "file.cache",
                true
        );

    }

    Path getCachePath(final RemoteCachingPath path) throws ExecutionException, IOException {
        final RemoteFileStore remoteFileStore = getFileStore(path);
        if (remoteFileStore instanceof PeerToPeerCacheFileStore) {
            return ((PeerToPeerCacheFileStore) remoteFileStore).getCachedPath(path);
        }
        return fileCache.cache(path);
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(fileStore, LOGGER);
        if (peerToPeerCacheFileStore != null) {
            Closeables2.closeQuietly(peerToPeerCacheFileStore, LOGGER);
        }
        fileCache.close();
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public String getSeparator() {
        return RemoteCachingPath.PATH_SEPARATOR_STR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.<Path>singletonList(new RemoteCachingPath(this, RemoteCachingPath.PATH_SEPARATOR_STR));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        final ImmutableList.Builder<FileStore> builder = ImmutableList.<FileStore>builder().add(fileStore.getBackingFileStore());
        if (peerToPeerCacheFileStore != null) {
            builder.add(peerToPeerCacheFileStore);
        }
        return builder.build();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("imhotep");
    }

    @Nullable
    ImhotepFileAttributes getFileAttributes(final RemoteCachingPath path) throws IOException {
        final RemoteFileStore remoteFileStore = getFileStore(path);
        final RemoteFileStore.RemoteFileAttributes remoteAttributes = remoteFileStore.getRemoteAttributes(path);
        if (remoteAttributes == null) {
            return null;
        }

        return new ImhotepFileAttributes(remoteAttributes.getSize(), remoteAttributes.isDirectory());
    }

    @Override
    public Path getPath(final String first, final String... more) {
        return getPath(Joiner.on(RemoteCachingPath.PATH_SEPARATOR_STR).join(Lists.asList(first, more)));
    }

    private Path getPath(final String path) {
        if (PeerToPeerCachePath.isAbsolutePeerToPeerCachePath(path)) {
            return PeerToPeerCachePath.newPeerToPeerCachePath(this, path);
        } else {
            return new RemoteCachingPath(this, path);
        }
    }

    Iterable<RemoteFileStore.RemoteFileAttributes> listDirWithAttributes(final RemoteCachingPath path) throws IOException {
        final RemoteFileStore remoteFileStore = getFileStore(path);
        return remoteFileStore.listDir(path);
    }

    Iterable<RemoteCachingPath> listDir(final RemoteCachingPath path) throws IOException {
        final RemoteFileStore remoteFileStore = getFileStore(path);
        return Iterables.transform(remoteFileStore.listDir(path), new Function<RemoteFileStore.RemoteFileAttributes, RemoteCachingPath>() {
            @Override
            public RemoteCachingPath apply(final RemoteFileStore.RemoteFileAttributes remoteFileAttributes) {
                return remoteFileAttributes.getPath();
            }
        });
    }

    SeekableByteChannel newByteChannel(final RemoteCachingPath path) throws IOException {
        final RemoteFileStore remoteFileStore = getFileStore(path);
        final ScopedCacheFile scopedCacheFile;
        try {
            if (remoteFileStore instanceof PeerToPeerCacheFileStore) {
                scopedCacheFile = ((PeerToPeerCacheFileStore) remoteFileStore).getForOpen(path);
            } else {
                scopedCacheFile = fileCache.getForOpen(path);
            }
        } catch (final ExecutionException e) {
            throw new IOException("Failed to access cache file for " + path, e);
        }
        return new CloseHookedSeekableByteChannel(
                Files.newByteChannel(scopedCacheFile.getCachePath()),
                scopedCacheFile);
    }

    RemoteFileStore getFileStore(final RemoteCachingPath path) {
        if (path instanceof PeerToPeerCachePath) {
            if (peerToPeerCacheFileStore == null) {
                throw new UnsupportedOperationException("PeerToPeerCacheFileStore is not supported from the filesystem configuration");
            }
            return peerToPeerCacheFileStore;
        }
        return fileStore;
    }

    private class CloseHookedSeekableByteChannel implements SeekableByteChannel {
        private final SeekableByteChannel wrapped;
        private final ScopedCacheFile scopedCacheFile;

        CloseHookedSeekableByteChannel(final SeekableByteChannel wrapped, final ScopedCacheFile scopedCacheFile) {
            this.wrapped = wrapped;
            this.scopedCacheFile = scopedCacheFile;
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            return wrapped.read(dst);
        }

        @Override
        public int write(final ByteBuffer src) throws IOException {
            return wrapped.write(src);
        }

        @Override
        public long position() throws IOException {
            return wrapped.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition) throws IOException {
            return wrapped.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return wrapped.size();
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            return wrapped.truncate(size);
        }

        @Override
        public boolean isOpen() {
            return wrapped.isOpen();
        }

        @Override
        public void close() throws IOException {
            Closeables2.closeAll(LOGGER, scopedCacheFile, wrapped);
        }
    }

    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern) {
        final int idx = syntaxAndPattern.indexOf(':');
        if ((idx == -1) && (idx >= (syntaxAndPattern.length() - 1))) {
            throw new IllegalArgumentException("Unexpected syntax and pattern format: " + syntaxAndPattern);
        }
        final String syntax = syntaxAndPattern.substring(0, idx);
        if ("regex".equals(syntax)) {
            final String pattern = syntaxAndPattern.substring(idx + 1);
            final Pattern compiledPattern = Pattern.compile(pattern);
            return new PathMatcher() {
                @Override
                public boolean matches(final Path path) {
                    return compiledPattern.matcher(path.toString()).matches();
                }
            };
        } else {
            throw new UnsupportedOperationException("Unsupported syntax " + syntax);
        }
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

    public List<CachedDatasetSnapshot> getLocalCacheSnapshot() throws IOException {
        return fileCache.getCacheSnapshot();
    }

    public List<CachedDatasetSnapshot> getPeerToPeerCacheSnapshot() throws IOException {
        return peerToPeerCacheFileStore == null ? Collections.emptyList() : peerToPeerCacheFileStore.getLocalCacheSnapshot();
    }
}