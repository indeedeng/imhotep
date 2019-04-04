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

import com.indeed.imhotep.hadoopcommon.HDFSUtils;
import com.indeed.imhotep.hadoopcommon.KerberosUtils;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.io.Closeables2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
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
import java.util.Optional;

/**
 * @author darren
 */
class HdfsRemoteFileStore extends RemoteFileStore {
    private static final Logger LOGGER = Logger.getLogger(HdfsRemoteFileStore.class);
    private static final String HDFS_BASE_DIR = "/var/imhotep/";

    private final Path hdfsShardBasePath;
    private final FileSystem fs;
    private final MetricStatsEmitter statsEmitter;

    private HdfsRemoteFileStore(final Map<String, ?> configuration, final MetricStatsEmitter statsEmitter) throws IOException, URISyntaxException {
        this.statsEmitter = statsEmitter;
        final String basePath = (String) configuration.get("imhotep.fs.filestore.hdfs.root.uri");

        if (basePath != null) {
            hdfsShardBasePath = new Path(new URI(basePath));
        } else {
            hdfsShardBasePath = new Path(HDFS_BASE_DIR);
        }

        KerberosUtils.loginFromKeytab(configuration);

        final Configuration hdfsConfiguration = HDFSUtils.getOurHDFSConfiguration();
        fs = FileSystem.get(hdfsConfiguration);
        LOGGER.info("Using path " + hdfsShardBasePath + " on file system: " + fs);
        fs.access(hdfsShardBasePath, FsAction.READ_EXECUTE);
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
    Optional<java.nio.file.Path> getCachedPath(final RemoteCachingPath path) {
        return Optional.empty();
    }

    @Override
    Optional<ScopedCacheFile> getForOpen(final RemoteCachingPath path) {
        return Optional.empty();
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
        // below 2 lines replicate fs.copyToLocalFile(hdfsPath, new Path(destPath.toUri())) while giving us access to file size
        final FileStatus fileStatus = fs.getFileStatus(hdfsPath);
        FileUtil.copy(fs, fileStatus, FileSystem.getLocal(fs.getConf()), new Path(destPath.toUri()), false, true, fs.getConf());
        final long fileSize = fileStatus.getLen();
        reportFileDownload(statsEmitter, fileSize);
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
        reportFileDownload(statsEmitter, length);
        return stream;
    }

    @Override
    public String name() {
        return hdfsShardBasePath.toString();
    }

    static class Factory implements RemoteFileStore.Factory {
        @Override
        public RemoteFileStore create(final Map<String, ?> configuration, final MetricStatsEmitter statsEmitter) {
            try {
                return new HdfsRemoteFileStore(configuration, statsEmitter);
            } catch (final IOException|URISyntaxException e) {
                throw new IllegalArgumentException("Failed to initialize HDFS file store", e);
            }
        }
    }
}
