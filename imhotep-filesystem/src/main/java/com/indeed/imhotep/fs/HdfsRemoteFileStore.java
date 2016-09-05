package com.indeed.imhotep.fs;

import com.indeed.util.core.io.Closeables2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author darren
 */
class HdfsRemoteFileStore extends RemoteFileStore {
    private static final Logger LOGGER = Logger.getLogger(HdfsRemoteFileStore.class);
    static final String HDFS_BASE_DIR = "/var/imhotep/";

    private final Path hdfsShardBasePath;
    private final FileSystem fs;

    private HdfsRemoteFileStore(final Map<String, ?> configuration) throws IOException, URISyntaxException {
        final String basePath = (String) configuration.get("imhotep.fs.filestore.hdfs.root-uri");

        if (basePath != null) {
            hdfsShardBasePath = new Path(new URI(basePath));
        } else {
            hdfsShardBasePath = new Path(HDFS_BASE_DIR);
        }

        fs = FileSystem.get(new Configuration());
    }

    private Path getHdfsPath(final RemoteCachingPath path) {
        Path result = hdfsShardBasePath;
        for (final java.nio.file.Path component : path.normalize()) {
            result = new Path(result, component.toString());
        }
        return result;
    }

    private static RemoteFileAttributes toAttributes(final RemoteCachingPath path, final FileStatus status) {
        return new RemoteFileAttributes(path, status.isFile() ? status.getLen() : -1, status.isFile());
    }

    @Override
    public List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final Path hdfsPath = getHdfsPath(path);
        final FileStatus[] statuses;
        try {
            statuses = fs.listStatus(hdfsPath);
        } catch (final FileNotFoundException e) {
            throw new NoSuchFileException(path.toString(), hdfsPath.toString(), e.toString());
        }

        final List<RemoteFileAttributes> results = new ArrayList<>();
        for (final FileStatus status : statuses) {
            if (status.getPath().equals(hdfsPath) && !status.isDirectory()) {
                throw new NotDirectoryException(path.toString());
            }
            results.add(toAttributes(path.resolve(status.getPath().getName()), status));
        }
        return results;
    }

    @Override
    public RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        final Path hdfsPath = getHdfsPath(path);
        try {
            final FileStatus status = fs.getFileStatus(hdfsPath);
            return toAttributes(path, status);
        } catch (final FileNotFoundException e) {
            throw new NoSuchFileException(path.toString(), hdfsPath.toString(), e.toString());
        }
    }

    @Override
    public void downloadFile(final RemoteCachingPath srcPath, final java.nio.file.Path destPath) throws IOException {
        final Path hdfsPath = getHdfsPath(srcPath);
        fs.copyToLocalFile(hdfsPath, new Path(destPath.toUri()));
    }

    @Override
    public InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws IOException {
        final Path hdfsPath = getHdfsPath(path);
        final FSDataInputStream stream = fs.open(hdfsPath);
        try {
            stream.seek(startOffset);
        } catch (final IOException e) {
            Closeables2.closeQuietly(stream, LOGGER);
            throw new IOException("Failed to open " + path + " with offset " + startOffset, e);
        }
        return stream;
    }

    @Override
    public String name() {
        return hdfsShardBasePath.toString();
    }

    static class Builder implements RemoteFileStore.Builder {
        @Override
        public RemoteFileStore build(final Map<String, ?> configuration) {
            try {
                return new HdfsRemoteFileStore(configuration);
            } catch (final IOException|URISyntaxException e) {
                throw new IllegalArgumentException("Failed to initialize HDFS file store", e);
            }
        }
    }
}
