package com.indeed.imhotep.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by darren on 2/26/16.
 */
public class HdfsDirLister implements DirLister {
    private static final Logger log = Logger.getLogger(HdfsDirLister.class);
    static final String DEFAULT_HDFS_SHARD_BASE_PATH = "/var/imhotep/";

    private final FileSystem fs;
    private final String hdfsShardBasePath;

    public HdfsDirLister(String basePath) throws IOException {
//        KerberosUtils.loginFromKeytab(props);

        // props.getString("squall.shardmanager.hdfs.shard.path", DEFAULT_HDFS_SHARD_BASE_PATH);
        if (basePath != null) {
            this.hdfsShardBasePath = basePath;
        } else {
            this.hdfsShardBasePath = DEFAULT_HDFS_SHARD_BASE_PATH;
        }

        this.fs = FileSystem.get(new Configuration());
        if (!(fs instanceof DistributedFileSystem)) {
            log.error("hadoop FileSystem instance is of type " + fs.getClass().getSimpleName()
                             + " at URI " + fs.getUri()
                             + ", Hadoop is probably not configured properly\n");
        }
    }

    private Path getHdfsPath(String pathStr) {
        final String relPath;
        if (pathStr.charAt(0) == '/') {
            relPath = pathStr.substring(1);
        } else {
            relPath = pathStr;
        }

        return new Path(hdfsShardBasePath, relPath);
    }

    @Override
    public RemoteIterator<String> listSubdirectories(String path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RemoteIterator<String> listFiles(String path) throws IOException {
        final RemoteIterator<LocatedFileStatus> iter;
        final Path hdfsPath = getHdfsPath(path);

        iter = fs.listFiles(hdfsPath, false);
        return new RemoteIterator<String>() {
            @Override
            public boolean hasNext() throws IOException {
                return iter.hasNext();
            }

            @Override
            public String next() throws IOException {
                return iter.next().getPath().getName();
            }
        };
    }

    @Override
    public RemoteIterator<String> listFilesAndDirectories(String path) throws IOException {
        final Path hdfsPath = getHdfsPath(path);
        final RemoteIterator<LocatedFileStatus> lfsIter;

        lfsIter = fs.listLocatedStatus(hdfsPath);
        return new RemoteIterator<String>() {
            @Override
            public boolean hasNext() throws IOException {
                return lfsIter.hasNext();
            }

            @Override
            public String next() throws IOException {
                final LocatedFileStatus lfs = lfsIter.next();
                return lfs.getPath().getName();
            }
        };
    }

    @Override
    public InputStream getInputStream(String path, long startOffset, long length) throws
            IOException {
        final Path hdfsPath = getHdfsPath(path);
        final FSDataInputStream stream = fs.open(hdfsPath);

        stream.seek(startOffset);
        return stream;
    }

}
