package com.indeed.imhotep.fs;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.connection.ImhotepConnection;
import com.indeed.imhotep.connection.ImhotepConnectionPool;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.io.Closeables2;
import jdk.internal.util.xml.impl.Input;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author xweng
 */
public class P2PCachingFileStore extends RemoteFileStore {
    private static final Logger logger = Logger.getLogger(P2PCachingFileStore.class);
    private static final int CONNECTION_FROM_POOL_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);

    private final LocalFileCache fileCache;
    private final MetricStatsEmitter statsEmitter;
    private Path cacheRootPath;

    public P2PCachingFileStore(final RemoteCachingFileSystem fs,
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

    Path getCachedPath(final RemoteCachingPath path) throws ExecutionException, IOException {
        return fileCache.cache(path);
    }

    LocalFileCache.ScopedCacheFile getOrOpen(final RemoteCachingPath path) throws ExecutionException {
        return fileCache.getForOpen(path);
    }

    @Override
    List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        throw new UnsupportedOperationException("You need to implement this");
    }

    @Override
    RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        throw new UnsupportedOperationException("You need to implement this");
    }

    @Override
    void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        downloadFileImpl((P2PCachingPath) srcPath, destPath);
    }

    @Override
    InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        final Path cachedPath;
        try {
            cachedPath = getCachedPath(path);
        } catch (final ExecutionException e) {
            throw new IOException("Failed to access cache file for " + path, e);
        }

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
    public String name() { return cacheRootPath.toString(); }

    private void downloadFileImpl(final P2PCachingPath srcPath, final Path destPath) throws IOException {
        final ImhotepConnectionPool pool = ImhotepConnectionPool.INSTANCE;
        final String localFilePath = srcPath.getRealPath().toUri().toString();

        final InputStream fileInputStream = pool.withConnection(srcPath.getPeerHost(), CONNECTION_FROM_POOL_TIMEOUT, connection -> {
            final Socket socket = connection.getSocket();
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final ImhotepRequest newRequest = ImhotepRequest.newBuilder()
                    .setRequestType(ImhotepRequest.RequestType.GET_SHARD_FILE)
                    .setShardFilePath(localFilePath)
                    .build();

            ImhotepProtobufShipping.sendProtobuf(newRequest, os);
            final ImhotepResponse imhotepResponse = ImhotepProtobufShipping.readResponse(is);
            if (imhotepResponse.getResponseCode() != ImhotepResponse.ResponseCode.OK) {
                // TODO: do some logs and handle the error message
            }
            return ByteStreams.limit(is, imhotepResponse.getFileLength());
        });

        try (final OutputStream outputStream = Files.newOutputStream(destPath)) {
            IOUtils.copy(fileInputStream, outputStream);
        }
        // TODO: do some stats record
    }
}
