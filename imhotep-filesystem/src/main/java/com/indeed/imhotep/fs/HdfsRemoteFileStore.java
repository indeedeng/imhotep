package com.indeed.imhotep.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by darren on 4/7/16.
 */
public class HdfsRemoteFileStore extends RemoteFileStore {
    private static final Logger log = Logger.getLogger(HdfsRemoteFileStore.class);
    static final String DEFAULT_HDFS_SHARD_BASE_PATH = "/var/imhotep/";

    private final Path hdfsShardBasePath;
    private final FileSystem fs;

    public HdfsRemoteFileStore(Map<String, String> settings) throws IOException {
        final String basePath = settings.get("hdfs-base-path");
//        KerberosUtils.loginFromKeytab(props);

        // props.getString("squall.shardmanager.hdfs.shard.path", DEFAULT_HDFS_SHARD_BASE_PATH);
        if (basePath != null) {
            this.hdfsShardBasePath = new Path(basePath);
        } else {
            this.hdfsShardBasePath = new Path(DEFAULT_HDFS_SHARD_BASE_PATH);
        }

        this.fs = FileSystem.get(new Configuration());
        if (!(fs instanceof DistributedFileSystem)) {
            log.error("hadoop FileSystem instance is of type " + fs.getClass().getSimpleName()
                              + " at URI " + fs.getUri()
                              + ", Hadoop is probably not configured properly\n");
        }
    }

    private Path getHdfsPath(RemoteCachingPath path) {
        final String relPath = path.normalize().relativize(path.getRoot()).toString();
        return new Path(hdfsShardBasePath, relPath);
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
    public ArrayList<RemoteFileInfo> listDir(RemoteCachingPath path) {
        final ArrayList<RemoteFileInfo> results = new ArrayList<>(100);
        try {
            final Path hdfsPath = getHdfsPath(path);
            final FileStatus statuses[] = fs.listStatus(hdfsPath);
            for (FileStatus status : statuses) {
                results.add(new RemoteFileInfo(status.getPath().getName(),
                                               status.isFile() ? status.getLen() : -1,
                                               status.isFile()));
            }
            return results;
        } catch (FileNotFoundException e) {
            // path does not exist. Return empty list
            return results;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public RemoteFileInfo readInfo(String shardPath) {
        try {
            final Path hdfsPath = getHdfsPath(shardPath);
            final FileStatus status = fs.getFileStatus(hdfsPath);
            return new RemoteFileInfo(shardPath, status.isFile() ? status.getLen() : -1, status.isFile());
        } catch (FileNotFoundException e) {
            // path does not exist.
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void downloadFile(RemoteCachingPath path, java.nio.file.Path tmpPath) throws IOException {
        final Path hdfsPath = getHdfsPath(path);
        fs.copyToLocalFile(false, hdfsPath, new Path(tmpPath.toUri()), true);
    }

    @Override
    public InputStream getInputStream(String path, long startOffset, long length) throws
            IOException {
        final Path hdfsPath = getHdfsPath(path);
        final FSDataInputStream stream = fs.open(hdfsPath);

        stream.seek(startOffset);
        return stream;
    }

    @Override
    public String name() {
        return "HDFS File Store";
    }

    @Override
    public String type() {
        return "Remote File Store";
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        return null;
    }

}
