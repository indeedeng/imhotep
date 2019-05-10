package com.indeed.imhotep.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.indeed.imhotep.SlotTiming;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.connection.ImhotepConnection;
import com.indeed.imhotep.connection.ImhotepConnectionPool;
import com.indeed.imhotep.connection.ImhotepConnectionPoolWrapper;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributeMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildIOExceptionFromResponse;
import static com.indeed.imhotep.utils.ImhotepExceptionUtils.buildImhotepKnownExceptionFromResponse;

/**
 * @author xweng
 */
public class PeerToPeerCacheFileStore extends RemoteFileStore {
    private static final Logger logger = Logger.getLogger(PeerToPeerCacheFileStore.class);
    private static final int FETCH_CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);
    private static final int REMOTE_ATTRIBUTES_CACHE_CAPACITY = 65536;
    private static final ImhotepConnectionPool CONNECTION_POOL = ImhotepConnectionPoolWrapper.INSTANCE;

    private final LocalFileCache fileCache;
    private final MetricStatsEmitter statsEmitter;
    private final Path cacheRootPath;
    private final LoadingCache<PeerToPeerCachePath, RemoteFileAttributes> remoteFileAttributesCache;

    PeerToPeerCacheFileStore(final RemoteCachingFileSystem fs,
                             final Map<String, ?> configuration,
                             final MetricStatsEmitter statsEmitters) throws IOException {
        statsEmitter = statsEmitters;
        cacheRootPath = Paths.get(URI.create((String) configuration.get("imhotep.fs.p2p.cache.root.uri")));

        fileCache = new LocalFileCache(
                fs,
                cacheRootPath,
                Long.parseLong((String) configuration.get("imhotep.fs.p2p.cache.size.gb")) * 1024 * 1024 * 1024,
                Integer.parseInt((String) configuration.get("imhotep.fs.p2p.cache.block.size.bytes")),
                new LocalFileCache.CacheFileLoader() {
                    @Override
                    public void load(final RemoteCachingPath src, final Path dest) throws IOException {
                        downloadFile(src, dest);
                    }
                },
                statsEmitter,
                "p2p.cache",
                false
        );

        remoteFileAttributesCache = CacheBuilder.newBuilder()
                .maximumSize(REMOTE_ATTRIBUTES_CACHE_CAPACITY)
                .build(
                        new CacheLoader<PeerToPeerCachePath, RemoteFileAttributes>() {
                            @Override
                            public RemoteFileAttributes load(final PeerToPeerCachePath path) throws IOException {
                                return getRemoteAttributesImpl(path);
                            }
                        }
                );
    }

    Path getCachedPath(final RemoteCachingPath path) throws IOException {
        try {
            return fileCache.cache(path);
        } catch (final ExecutionException e) {
            throw Throwables2.propagate(e.getCause(), IOException.class, RuntimeException.class);
        }
    }

    ScopedCacheFile getForOpen(final RemoteCachingPath path) throws IOException {
        try {
            return fileCache.getForOpen(path);
        } catch (final ExecutionException e) {
            throw Throwables2.propagate(e.getCause(), IOException.class, RuntimeException.class);
        }
    }

    @Override
    List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final PeerToPeerCachePath peerToPeerCachePath = (PeerToPeerCachePath) path;
        final Host remoteHost = peerToPeerCachePath.getRemoteHost();
        final String localFileUri = peerToPeerCachePath.getRealPath().toUri().toString();
        final ImhotepRequest.Builder newRequestBuilder = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_FILE_ATTRIBUTES)
                .setShardFileUri(localFileUri);

        final List<FileAttributeMessage> attributeList = handleRequest(newRequestBuilder, peerToPeerCachePath,
                response -> response.getSubFilesAttributesList());
        return attributeList.stream()
                .map(attr -> {
                    final RemoteFileAttributes remoteAttributes = new RemoteFileAttributes(
                        PeerToPeerCachePath.toPeerToPeerCachePath(
                                peerToPeerCachePath.getRoot(),
                                attr.getPath(),
                                remoteHost),
                        attr.getSize(),
                        !attr.getIsDirectory());

                    // put all paths from listDir into the cache.
                    // It could avoid future remoteAttributes requests under the directory although there are some redundant fields path in the cache.
                    // check if the path has been cached in case of frequent replacement
                    final PeerToPeerCachePath cacheKey = (PeerToPeerCachePath) remoteAttributes.getPath();
                    if (remoteFileAttributesCache.getIfPresent(cacheKey) == null) {
                        remoteFileAttributesCache.put(cacheKey, remoteAttributes);
                    }
                    return remoteAttributes;
                })
                .collect(Collectors.toList());
    }

    @Override
    RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        final PeerToPeerCachePath peerToPeerCachePath = (PeerToPeerCachePath) path;
        try {
            return remoteFileAttributesCache.get(peerToPeerCachePath);
        } catch (final ExecutionException e) {
            throw Throwables2.propagate(e.getCause(), IOException.class, RuntimeException.class);
        }
    }

    private RemoteFileAttributes getRemoteAttributesImpl(final PeerToPeerCachePath peerToPeerCachePath) throws IOException {
        final String realFileUri = peerToPeerCachePath.getRealPath().toUri().toString();
        final ImhotepRequest.Builder newRequestBuilder = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE_ATTRIBUTES)
                .setShardFileUri(realFileUri);

        final FileAttributeMessage attributes = handleRequest(newRequestBuilder, peerToPeerCachePath,
                response -> response.getFileAttributes());
        return new RemoteFileAttributes(peerToPeerCachePath, attributes.getSize(), !attributes.getIsDirectory());
    }

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        try (final InputStream inputStream = getInputStream(srcPath)) {
            try (final OutputStream fileOutputStream = Files.newOutputStream(destPath)) {
                IOUtils.copy(inputStream, fileOutputStream);
            }
        }
    }

    @Override
    InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        final InputStream inputStream = getInputStream(path);
        final long skipped;
        try {
            skipped = inputStream.skip(startOffset);
        } catch (final IOException e) {
            Closeables2.closeQuietly(inputStream, logger);
            throw new IOException("Failed to open " + path + " with offset " + startOffset, e);
        }

        if (skipped != startOffset) {
            throw new IOException("Could not move offset for path " + path + " by " + startOffset);
        }
        return inputStream;
    }

    @Override
    public String name() {
        return cacheRootPath.toString();
    }

    private InputStream getInputStream(final RemoteCachingPath path) throws IOException {
        final PeerToPeerCachePath srcPath = (PeerToPeerCachePath) path;
        final String realFileUri = srcPath.getRealPath().toUri().toString();
        final long downloadStartMillis = System.currentTimeMillis();
        final Host host = srcPath.getRemoteHost();
        final ImhotepRequest.Builder newRequestBuilder = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFileUri(realFileUri);

        // here the connection will be closed with the closure of ConnectionInputStream
        final ImhotepConnection connection = CONNECTION_POOL.getConnection(host, FETCH_CONNECTION_TIMEOUT);
        final Closeable unlockCloseable = TaskScheduler.CPUScheduler.temporaryUnlock();
        try {
            final Socket socket = connection.getSocket();
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final ImhotepResponse response = sendRequest(newRequestBuilder, is, os, host);
            reportFileDownload(response.getFileLength(), System.currentTimeMillis() - downloadStartMillis);
            return new ConnectionInputStream(ByteStreams.limit(is, response.getFileLength()), connection, unlockCloseable);
        } catch (final Throwable t) {
            connection.markAsInvalid();
            unlockCloseable.close();
            throw t;
        }
    }

    /**
     * A generic interface to handle requests in the p2pCachingStore
     * @param requestBuilder
     * @param peerToPeerCachePath
     * @param function is the method to get results from response and socket inputstream(downloading files)
     * @param <R>
     * @return R
     * @throws IOException
     */
    private <R> R handleRequest(
            final ImhotepRequest.Builder requestBuilder,
            final PeerToPeerCachePath peerToPeerCachePath,
            final ThrowingFunction<ImhotepResponse, R> function) throws IOException {
        final Host srcHost = peerToPeerCachePath.getRemoteHost();

        try (final Closeable ignored = TaskScheduler.CPUScheduler.temporaryUnlock()) {
            return CONNECTION_POOL.withConnection(srcHost, FETCH_CONNECTION_TIMEOUT, connection -> {
                final Socket socket = connection.getSocket();
                final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
                final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
                final ImhotepResponse response = sendRequest(requestBuilder, is, os, srcHost);
                return function.apply(response);
            });
        }
    }

    private ImhotepResponse sendRequest(
            final ImhotepRequest.Builder requestBuilder,
            final InputStream is,
            final OutputStream os,
            final Host host) throws IOException {
        appendTaskInfoToRequest(requestBuilder);

        ImhotepProtobufShipping.sendProtobuf(requestBuilder.build(), os);
        final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);
        if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
            throw buildImhotepKnownExceptionFromResponse(imhotepResponse, host.hostname, host.getPort(), null);
        }
        if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
            throw buildIOExceptionFromResponse(imhotepResponse, host.getHostname(), host.getPort(), null);
        }

        readAndAddSlotTiming(imhotepResponse);
        return imhotepResponse;
    }

    private interface ThrowingFunction<T, R> {
        R apply(T t) throws IOException;
    }

    private void reportFileDownload(
            final long size,
            final long duration) {
        statsEmitter.histogram("p2p.file.downloaded.size", size);
        statsEmitter.histogram("p2p.file.downloaded.time", duration);

        final ImhotepTask currentThreadTask = ImhotepTask.THREAD_LOCAL_TASK.get();
        if (currentThreadTask == null || currentThreadTask.getSession() == null) {
            return;
        }
        currentThreadTask.getSession().addDownloadedBytesInPeerToPeerCache(size);
    }

    /**
     * Append the username and client to requests before sending
     */
    private void appendTaskInfoToRequest(final ImhotepRequest.Builder builder) {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        if (task == null) {
            return;
        }

        builder.setUsername(task.getUserName())
               .setClientName(task.getClientName())
               .setSessionPriority(task.getPriority());
    }

    /**
     * Extract slot timing statistics from response and update them to session
     */
    private void readAndAddSlotTiming(final ImhotepResponse response) {
        if (!response.hasSlotTiming()) {
            return;
        }

        final ImhotepTask currentThreadTask = ImhotepTask.THREAD_LOCAL_TASK.get();
        if (currentThreadTask == null || currentThreadTask.getSession() == null) {
            return;
        }

        final SlotTiming slotTiming = currentThreadTask.getSession().getSlotTiming();
        slotTiming.addFromSlotTimingMessage(response.getSlotTiming());
    }

    /**
     * A wrapped socket inputstream holding the pooled connection and unlockCloseable, which won't close the inner InputStream
     * to keep socket connected.
     * When the current stream is closed
     * 1. the connection will be closed to return the socket back
     * 2. the unlockCloseable will be closed to schedule new tasks
     */
    private static class ConnectionInputStream extends FilterInputStream {
        private final ImhotepConnection connection;
        private final Closeable unlockCloseable;

        ConnectionInputStream(final InputStream is, final ImhotepConnection connection, final Closeable unlockCloseable) {
            super(is);
            this.connection = connection;
            this.unlockCloseable = unlockCloseable;
        }

        @Override
        public void close() throws IOException {
            Closeables2.closeAll(logger, connection, unlockCloseable);
        }
    }
}
