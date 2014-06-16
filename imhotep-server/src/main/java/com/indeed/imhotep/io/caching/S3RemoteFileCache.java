package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

public class S3RemoteFileCache extends RemoteFileCache {
    private String localPrefix;
    private String s3bucket;
    private String s3prefix;
    private AmazonS3Client client;
    private LoadingCache<String, File> cache;

    public S3RemoteFileCache(Properties properties) throws IOException {
        final String s3key;
        final String s3secret;
        final BasicAWSCredentials cred;
        final int cacheSize;
        
        localPrefix = properties.getProperty("file.caching.mountpoint");
        if (localPrefix.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            localPrefix = localPrefix.substring(0, localPrefix.length() - DELIMITER.length());
        }
        /* create directory if it does not already exist */
        File cachedir = new File(localPrefix);
        cachedir.mkdir();
        
        s3bucket = properties.getProperty("file.caching.s3.bucket");
        s3prefix = properties.getProperty("file.caching.s3.prefix", null);
        if (s3prefix != null) {
            s3prefix = RemoteFileCache.cleanupPath(s3prefix);
        }
        s3key = properties.getProperty("file.caching.s3.key");
        s3secret = properties.getProperty("file.caching.s3.secret");
        cacheSize = Integer.valueOf(properties.getProperty("file.caching.cacheSize.inMB"));
        cred = new BasicAWSCredentials(s3key, s3secret);

        client = new AmazonS3Client(cred);
        
        cache =
                CacheBuilder.newBuilder()
                            .maximumWeight(cacheSize * 1024)
                            .weigher(new Weigher<String, File>() {
                                public int weigh(String path, File cachedFile) {
                                    return (int)(cachedFile.length() / 1024);
                                }
                            })
                            .removalListener(new RemovalListener<String, File>() {
                                public void onRemoval(RemovalNotification<String, File> rn) {
                                    removeFile(rn.getValue());
                                }
                            })
                            .build(new CacheLoader<String, File>() {
                                public File load(String path) throws Exception {
                                    return downloadFile(path);
                                }
                            });

        scanExistingFiles();
    }
    
    private void scanExistingFiles() throws IOException {
        final File cacheRoot;
        final Iterator<File> filesInCache;
        final int prefixLen;
        
        cacheRoot = new File(localPrefix);
        prefixLen = cacheRoot.getCanonicalPath().length() + DELIMITER.length();
        filesInCache = FileUtils.iterateFiles(cacheRoot, 
                                              TrueFileFilter.INSTANCE, 
                                              TrueFileFilter.INSTANCE);
        while (filesInCache.hasNext()) {
            final File cachedFile = filesInCache.next();
            final String path = cachedFile.getCanonicalPath();
            String cachePath = path.substring(prefixLen);
            cache.put(cachePath, cachedFile);
        }
    }
    
    private void removeFile(File cachedFile) {
        cachedFile.delete();
    }
    
    private File downloadFile(String path) throws FileNotFoundException {
        final String fullPath;
        final String cachePath;
        final ObjectMetadata metadata;
        final File file;
        
        fullPath = (s3prefix != null) ? s3prefix + DELIMITER + path : path;
        cachePath = (localPrefix != null) ? localPrefix + DELIMITER + path : path;
        file = new File(cachePath);
        /* create all the directories on the path to the file */
        file.getParentFile().mkdirs();

        try {
            metadata = client.getObject(new GetObjectRequest(s3bucket, fullPath), file);
        } catch(AmazonS3Exception e) {
            throw new FileNotFoundException();
        }

        return file;
    }
    
    String getS3path(String path) {
        final String s3Path;
        
        path = RemoteFileCache.cleanupPath(path);
        s3Path = (s3prefix != null) ? s3prefix + DELIMITER + path : path;
        return s3Path;
    }
    
    String getS3bucket() {
        return s3bucket;
    }
    
    AmazonS3Client getS3client() {
        return client;
    }

    
    @Override
    public File getFile(String path) throws ExecutionException {
        
        /* remove starting delimiter */
        if (path.startsWith(DELIMITER)) {
            path = path.substring(DELIMITER.length());
        }
        
        return cache.get(path);
    }
    
    @Override
    public boolean contains(String path) {
        return cache.asMap().get(path) != null;
    }

    public String getLocalPath(String path) {
        final String localPath;
        
        path = RemoteFileCache.cleanupPath(path);
        localPath = localPrefix + DELIMITER + path;
        return localPath;
    }
}
