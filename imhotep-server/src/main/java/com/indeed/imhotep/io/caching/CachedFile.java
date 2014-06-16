package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public abstract class CachedFile {
    public static final String DELIMITER = "/";
    public static final int CHAR_DELIMITER = '/';
    private static final int TYPE_NOOP = 1;
    private static final int TYPE_S3 = 2;
    private static final int TYPE_HDFS = 3;
    private static boolean initialized = false;
    private static int cacheType;
    private static RemoteFileCache cache;
    

    public static final CachedFile create(String path) {
        if (! initialized) {
            try {
                CachedFile.init(null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        switch (cacheType) {
        case TYPE_NOOP:
            return new NoOpCachedFile(cache, path);
        case TYPE_S3:
            return new S3CachedFile(cache, path);
        case TYPE_HDFS:
            return new HDFSCachedFile(cache, path);
        default:
            throw new RuntimeException("Unknow remote file cache type");
        }
    }
    
    public static final void init(Properties properties) throws IOException {

        if (properties == null ) {
            final InputStream in;

            properties = new Properties();
            in = ClassLoader.getSystemResourceAsStream("file-caching.properties");
            if (in != null) {
                try {
                    properties.load(in);
                } catch (IOException e) {
                    e.printStackTrace();
                    properties = noCachingProps();
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) { }
                }
            } else {
                properties = noCachingProps();
            }
        }
        
        final String type = properties.getProperty("file.caching.type","NOOP");
        if (type.equals("NOOP")) {
            cacheType = TYPE_NOOP;
            cache = new NoOpRemoteFileCache(properties);
        } else if (type.equals("S3")) {
            cacheType = TYPE_S3;
            cache = new S3RemoteFileCache(properties);
        } else if (type.equals("HDFS")) {
            cacheType = TYPE_HDFS;
            cache = new HDFSRemoteFileCache(properties);
        }
        
        initialized = true;
    }
    
    private static Properties noCachingProps() {
        final Properties result = new Properties();
        
        result.setProperty("cache.type", "NOOP");
        return result;
    }

    /*
     * Remove leading and trailing delimiters
     */
    protected static String cleanupPath(String path) {
        if (path.startsWith(DELIMITER)) {
            /* remove delimiter from the beginning */
            path = path.substring(DELIMITER.length());
        }
        if (path.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            path = path.substring(0, path.length() - DELIMITER.length());
        }
        return path;
    }
    
    public static String buildPath(String ... parts) {
        if (parts.length == 0) return null;
        if (parts.length == 1) return parts[0];
        
        String path = parts[0];
        for (int i = 1; i < parts.length; i++) {
            path += DELIMITER + parts[i];
        }
        
        return path;
    }

    
    public abstract boolean exists();
    public abstract boolean isFile();
    public abstract boolean isDirectory();
    public abstract String getName();
    public abstract String[] list();
    public abstract CachedFile[] listFiles();
    public abstract File loadFile() throws IOException;
    public abstract File loadDirectory() throws IOException;

    public abstract boolean isCached();

    public abstract String getCanonicalPath() throws IOException;

    public abstract long length();


}
