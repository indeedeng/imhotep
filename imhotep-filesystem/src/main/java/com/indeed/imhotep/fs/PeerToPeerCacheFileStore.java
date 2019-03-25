package com.indeed.imhotep.fs;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.connection.ImhotepConnectionPool;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.FileAttributeMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

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

    private final LocalFileCache fileCache;
    private final MetricStatsEmitter statsEmitter;
    private final Path cacheRootPath;

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
    }

    @Override
    Optional<Path> getCachedPath(final RemoteCachingPath path) throws IOException {
        try {
            return Optional.of(fileCache.cache(path));
        } catch (final ExecutionException e) {
            throw Throwables2.propagate(e, IOException.class, RuntimeException.class);
        }
    }

    @Override
    Optional<ScopedCacheFile> getForOpen(final RemoteCachingPath path) throws IOException {
        try {
            return Optional.of(fileCache.getForOpen(path));
        } catch (final ExecutionException e) {
            throw Throwables2.propagate(e, IOException.class, RuntimeException.class);
        }
    }

    @Override
    List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final PeerToPeerCachePath peerToPeerCachePath = (PeerToPeerCachePath) path;
        final Host remoteHost = peerToPeerCachePath.getPeerHost();
        final String localFilePath = peerToPeerCachePath.getRealPath().toUri().toString();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(localFilePath)
                .build();

        final List<FileAttributeMessage> attributeList = handleRequest(newRequest, peerToPeerCachePath,
                (response, is) -> response.getSubFilesAttributesList());
        return attributeList.stream()
                .map(attr -> new RemoteFileAttributes(
                        PeerToPeerCachePath.toPeerToPeerCachePath(
                                peerToPeerCachePath.getRoot(),
                                attr.getPath(),
                                remoteHost),
                        attr.getSize(),
                        !attr.getIsDirectory()))
                .collect(Collectors.toList());
    }

    @Override
    RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        final PeerToPeerCachePath peerToPeerCachePath = (PeerToPeerCachePath) path;
        final String realFilePath = peerToPeerCachePath.getRealPath().toUri().toString();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(realFilePath)
                .build();

        final FileAttributeMessage attributes = handleRequest(newRequest, peerToPeerCachePath,
                (response, is) -> response.getFileAttributes());
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
        final String realFilePath = srcPath.getRealPath().toUri().toString();
        final long downloadStartMillis = System.currentTimeMillis();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFilePath(realFilePath)
                .build();

        return handleRequest(newRequest, srcPath, (response, is) -> {
            reportFileDownload(response.getFileLength(), System.currentTimeMillis() - downloadStartMillis);
            final InputStream rawInputStream = ByteStreams.limit(is, response.getFileLength());
            // rawInputStream won't be closed in case the socket is also closed
            return new FilterInputStream(rawInputStream) {
                @Override
                public void close() { }
            };
        });
    }

    /**
     * A generic interface to handle requests in the p2pCachingStore
     * @param request
     * @param peerToPeerCachePath
     * @param function is the method to get results from response and socket inputstream(downloading files)
     * @param <R>
     * @return R
     * @throws IOException
     */
    private <R> R handleRequest(
            final ImhotepRequest request,
            final PeerToPeerCachePath peerToPeerCachePath,
            final ThrowingFunction<ImhotepResponse, InputStream, R> function) throws IOException {
        final ImhotepConnectionPool pool = ImhotepConnectionPool.INSTANCE;
        final Host srcHost = peerToPeerCachePath.getPeerHost();

        return pool.withConnection(srcHost, FETCH_CONNECTION_TIMEOUT, connection -> {
            final Socket socket = connection.getSocket();
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());

            ImhotepProtobufShipping.sendProtobuf(request, os);
            final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);
            if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
                throw buildImhotepKnownExceptionFromResponse(imhotepResponse, srcHost.hostname, srcHost.getPort(), null);
            }
            if (imhotepResponse.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildIOExceptionFromResponse(imhotepResponse, srcHost.getHostname(), srcHost.getPort(), null);
            }
            return function.apply(imhotepResponse, is);
        });
    }

    private interface ThrowingFunction<K, T, R> {
        R apply(K k, T t) throws IOException;
    }

    private void reportFileDownload(
            final long size,
            final long duration) {
        statsEmitter.histogram("p2p.file.downloaded.size", size);
        statsEmitter.histogram("p2p.file.downloaded.time", duration);
    }
}
