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
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
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
public class P2PCachingFileStore extends RemoteFileStore {
    private static final Logger logger = Logger.getLogger(P2PCachingFileStore.class);
    private static final int FETCH_CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

    private final LocalFileCache fileCache;
    private final MetricStatsEmitter statsEmitter;
    private final Path cacheRootPath;

    P2PCachingFileStore(final RemoteCachingFileSystem fs,
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
                "p2p.cache"
        );
    }

    @Override
    Optional<Path> getCachedPath(final RemoteCachingPath path) throws IOException {
        try {
            return Optional.of(fileCache.cache(path));
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unexpected error while getting cache path for " + path, e);
        }
    }

    @Override
    Optional<LocalFileCache.ScopedCacheFile> getForOpen(final RemoteCachingPath path) {
        try {
            return Optional.of(fileCache.getForOpen(path));
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unexpected error while open cached file for " + path, e);
        }
    }

    LocalFileCache.ScopedCacheFile getOrOpen(final RemoteCachingPath path) throws ExecutionException {
        return fileCache.getForOpen(path);
    }

    @Override
    List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final P2PCachingPath p2PCachingPath = (P2PCachingPath) path;
        final Host remoteHost = p2PCachingPath.getPeerHost();
        final String localFilePath = p2PCachingPath.getRealPath().toUri().toString();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.LIST_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(localFilePath)
                .build();

        final List<FileAttributeMessage> attributeList = handleRequest(newRequest, p2PCachingPath,
                (response, is) -> response.getSubFilesAttributesList());
        return attributeList.stream()
                .map(attr -> new RemoteFileAttributes(
                        P2PCachingPath.toP2PCachingPath(
                                p2PCachingPath.getRoot(),
                                attr.getPath(),
                                remoteHost),
                        attr.getSize(),
                        !attr.getIsDirectory()))
                .collect(Collectors.toList());
    }

    @Override
    RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        final P2PCachingPath p2PCachingPath = (P2PCachingPath) path;
        final String realFilePath = p2PCachingPath.getRealPath().toUri().toString();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE_ATTRIBUTES)
                .setShardFilePath(realFilePath)
                .build();

        final FileAttributeMessage attributes = handleRequest(newRequest, p2PCachingPath,
                (response, is) -> response.getFileAttributes());
        return new RemoteFileAttributes(p2PCachingPath, attributes.getSize(), !attributes.getIsDirectory());
    }

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final P2PCachingPath p2PCachingPath = (P2PCachingPath) srcPath;
        // download only if files are in other servers
        if (!ownShard(p2PCachingPath)) {
            downloadFileImpl(p2PCachingPath, destPath);
        } else {
            try (final InputStream fileInputStream = Files.newInputStream(p2PCachingPath.getRealPath())) {
                try (final OutputStream outputStream = Files.newOutputStream(destPath)) {
                    IOUtils.copy(fileInputStream, outputStream);
                }
            }
        }
    }

    @Override
    InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        final Path cachedPath = getCachedPath(path).get();

        final InputStream is = Files.newInputStream(cachedPath);
        final long skipped;
        try {
            skipped = is.skip(startOffset);
        } catch (final IOException e) {
            Closeables2.closeQuietly(is, logger);
            throw new IOException("Failed to open " + path + " with offset " + startOffset, e);
        }

        if (skipped != startOffset) {
            throw new IOException("Could not move offset for path " + path + " by " + startOffset);
        }
        return is;
    }

    @Override
    public String name() {
        return cacheRootPath.toString();
    }

    private void downloadFileImpl(final P2PCachingPath srcPath, final Path destPath) throws IOException {
        final String realFilePath = srcPath.getRealPath().toUri().toString();
        final long downloadStartMillis = System.currentTimeMillis();
        final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                .setShardFilePath(realFilePath)
                .build();

        // fileInputStream won't be closed otherwise socket will be closed too
        final InputStream fileInputStream = handleRequest(newRequest, srcPath, (response, is) -> {
            reportFileDownload(response.getFileLength(), System.currentTimeMillis() - downloadStartMillis);
            return ByteStreams.limit(is, response.getFileLength());
        });

        try (final OutputStream fileOutputStream = Files.newOutputStream(destPath)) {
            IOUtils.copy(fileInputStream, fileOutputStream);
        }
    }

    /**
     * A generic interface to handle requests in the p2pCachingStore
     * @param request
     * @param p2PCachingPath
     * @param function is the method to get results from response and socket inputstream(downloading files)
     * @param <R>
     * @return
     * @throws IOException
     */
    private <R> R handleRequest(
            final ImhotepRequest request,
            final P2PCachingPath p2PCachingPath,
            final ThrowingFunction<ImhotepResponse, InputStream, R> function) throws IOException {
        final ImhotepConnectionPool pool = ImhotepConnectionPool.INSTANCE;
        final Host srcHost = p2PCachingPath.getPeerHost();

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

    // check if current server is the owner of that shard file
    private boolean ownShard(final P2PCachingPath path) throws UnknownHostException {
        final String remoteHostName = path.getPeerHost().getHostname();
        final InetAddress localHost = InetAddress.getLocalHost();

        return StringUtils.equals(remoteHostName, localHost.getHostName())
                || StringUtils.equals(remoteHostName, localHost.getHostAddress());
    }

    private void reportFileDownload(
            final long size,
            final long duration) {
        statsEmitter.histogram("p2p.file.downloaded.size", size);
        statsEmitter.histogram("p2p.file.downloaded.time", duration);
    }
}
