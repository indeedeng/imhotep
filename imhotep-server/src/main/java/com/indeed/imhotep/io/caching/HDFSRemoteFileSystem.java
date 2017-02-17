/*
 * Copyright (C) 2014 Indeed Inc.
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
package com.indeed.imhotep.io.caching;

import com.indeed.util.core.io.Closeables2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSRemoteFileSystem extends RemoteFileSystem {
    private static final Logger log = Logger.getLogger(HDFSRemoteFileSystem.class);

    private final RemoteFileSystemMounter mounter;
    private final String mountPoint;
    private final Path hdfsBasePath;
    private final FileSystem fs;

    HDFSRemoteFileSystem(Map<String,Object> settings, RemoteFileSystemMounter mounter)
            throws IOException {
        this.mounter = mounter;

        try {
            mountPoint = (String) settings.get("mountpoint");
            if (mountPoint != null) {
                hdfsBasePath = new Path(new URI(mountPoint));
            } else {
                hdfsBasePath = new Path("/var/imhotep/");
            }

            fs = FileSystem.get(new Configuration());
            log.info("Using path " + hdfsBasePath + " on file system: " + fs);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Path hdfsPath = new Path(hdfsBasePath, relativePath);
        fs.copyToLocalFile(hdfsPath, new Path(localFile.getPath()));
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        final File file = File.createTempFile("imhotep.hdfs.", ".cachedFile");
        copyFileInto(fullPath, file);
        return file;
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Path hdfsPath = new Path(hdfsBasePath, relativePath);
        try {
            final FileStatus status = fs.getFileStatus(hdfsPath);
            if (status.isDirectory()) {
                return new RemoteFileInfo(relativePath, RemoteFileInfo.TYPE_DIR);
            } else {
                return new RemoteFileInfo(relativePath, RemoteFileInfo.TYPE_FILE);
            }
        } catch (final IOException e) {
            log.error(e);
            return null;
        }
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Path hdfsPath = new Path(hdfsBasePath, relativePath);

        try {
            final FileStatus[] contents = fs.listStatus(hdfsPath);

            final List<RemoteFileInfo> results = new ArrayList<>();
            for (FileStatus status : contents) {
                if (status.getPath().equals(hdfsPath) && !status.isDirectory()) {
                    throw new IOException("Not a directory: " + fullPath);
                }
                if (status.isDirectory()) {
                    results.add(new RemoteFileInfo(status.getPath().getName(), RemoteFileInfo.TYPE_DIR));
                } else {
                    results.add(new RemoteFileInfo(status.getPath().getName(), RemoteFileInfo.TYPE_FILE));
                }
            }
            return results;
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    @Override
    public String getMountPoint() {
        return mountPoint;
    }

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Path hdfsPath = new Path(hdfsBasePath, relativePath);

        final Map<String, File> results = new HashMap<>(100);

        if (location == null) {
            location = File.createTempFile("hdfs", "remoteFile");
            location.delete();
            location.mkdir();
        }

        try {
            final FileStatus[] contents = fs.listStatus(hdfsPath);
            for (FileStatus status : contents) {
                if (!status.isDirectory()) {
                    File localFile = new File(location, status.getPath().getName());
                    localFile.getParentFile().mkdirs();
                    copyFileInto(status.getPath().toString(), localFile);
                    results.put(fullPath + status.getPath().getName(), localFile);
                }
            }
        } catch (IOException e) {
            log.error(e);
        }

        return results;
    }

    @Override
    public InputStream getInputStreamForFile(String fullPath, long startOffset, long maxReadLength) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Path hdfsPath = new Path(hdfsBasePath, relativePath);
        final FSDataInputStream stream = fs.open(hdfsPath);
        try {
            stream.seek(startOffset);
        } catch (final IOException e) {
            Closeables2.closeQuietly(stream, log);
            throw new IOException("Failed to open " + fullPath + " with offset " + startOffset, e);
        }
        return stream;
    }


}
