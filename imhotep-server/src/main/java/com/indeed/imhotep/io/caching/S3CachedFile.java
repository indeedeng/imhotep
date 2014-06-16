package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3CachedFile extends CachedFile {
    private static final int TYPE_UNKNOWN = 1;
    private static final int TYPE_FILE = 2;
    private static final int TYPE_DIR = 3;
    
    private S3RemoteFileCache cache;
    private String path;
    private int type;

    public S3CachedFile(RemoteFileCache cache, String path) {
        this.cache = (S3RemoteFileCache) cache;
        this.path = path;
        this.type = TYPE_UNKNOWN;
    }

    private long getObjectSizeinKB(String path) {
        final ObjectMetadata metadata;
        
        path = cleanupPath(path);
        
        metadata = getMetadata(path);
        return metadata.getContentLength() / 1024;
    }
    
    private ObjectMetadata getMetadata(String path) {
        final String fullPath;
        final ObjectMetadata metadata;
        final AmazonS3Client client = cache.getS3client();
        
        fullPath = cache.getS3path(path);
        
        try {
            metadata = client.getObjectMetadata(cache.getS3bucket(), fullPath);
        } catch(AmazonServiceException e) {
            return null;
        }
        
        return metadata;
    }
    
    @Override
    public boolean exists() {
        final ListObjectsRequest reqParams;
        final String prefix;
        final ObjectListing listing;
        final AmazonS3Client client = cache.getS3client();
        
        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(cache.getS3bucket());
        prefix = cache.getS3path(path);
        reqParams.setPrefix(prefix);
        reqParams.setMaxKeys(1);  // only need one to verify existence
        
        listing = client.listObjects(reqParams);
        
        if (listing.getObjectSummaries().size() == 0) {
            return false;
        }
        
        final String key;
        key = listing.getObjectSummaries().get(0).getKey();
        
        if (key.equals(prefix)) {
             type = TYPE_FILE;
             return true;
        }
        
        if (key.startsWith(prefix + DELIMITER)) {
            type = TYPE_DIR;
            return true;
        }
       
        return false;
    }

    @Override
    public boolean isFile() {
        final ObjectMetadata metadata;
        
        /* check if we have already looked up this file */
        if (type == TYPE_FILE) {
            return true;
        }
        if (type == TYPE_DIR) {
            return false;
        }
        
        metadata = getMetadata(path);
        if (metadata != null) {
            type = TYPE_FILE;
            return true;
        }
        
        return false;
    }

    @Override
    public boolean isDirectory() {
        final ListObjectsRequest reqParams;
        final String prefix;
        final ObjectListing listing;
        final AmazonS3Client client = cache.getS3client();
        
        /* check if we have already looked up this file */
        if (type == TYPE_FILE) {
            return false;
        }
        if (type == TYPE_DIR) {
            return true;
        }
        
        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(cache.getS3bucket());
        prefix = cache.getS3path(path) + DELIMITER;
        reqParams.setPrefix(prefix);
        reqParams.setMaxKeys(1);  // only need one to verify existence
        
        listing = client.listObjects(reqParams);
        
        if (listing.getObjectSummaries().size() == 0) {
            return false;
        }

        type = TYPE_DIR;
        return true;
    }
    
    @Override
    public String getName() {
        return path.substring(path.lastIndexOf(CHAR_DELIMITER) + 1);
    }
    
    private List<String> getFilenamesFromListing(ObjectListing listing, String prefix) {
        List<String> results = new ArrayList<String>(100);

        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            final String key = summary.getKey();
            final String filename;
            
            filename = key.substring(prefix.length());
            if (filename.length() == 0 || filename.contains(DELIMITER)) {
                throw new RuntimeException("Error parsing S3 object Key.  Key: " + key);
            }
            results.add(filename);
        }
        
        return results;
    }
    
    private List<String> getCommonPrefixFromListing(ObjectListing listing, String prefix) {
        List<String> results = new ArrayList<String>(100);

        for (String commonPrefix : listing.getCommonPrefixes()) {
            final String dirname;
            
            /* remove prefix and trailing delimiter */
            dirname = commonPrefix.substring(prefix.length(), 
                                             commonPrefix.length() - DELIMITER.length());
            if (dirname.length() == 0 || dirname.contains(DELIMITER)) {
                throw new RuntimeException("Error parsing S3 object prefix.  Prefix: " + 
                        commonPrefix);
            }
            results.add(dirname);
        }
        
        return results;
    }

    @Override
    public String[] list() {
        final ListObjectsRequest reqParams;
        final String prefix;
        final List<String> results = new ArrayList<String>(100);
        ObjectListing listing;
        final AmazonS3Client client = cache.getS3client();
        
        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(cache.getS3bucket());
        prefix = cache.getS3path(path) + DELIMITER;
        reqParams.setPrefix(prefix);
        reqParams.setDelimiter(DELIMITER);
        
        /* grab first set of keys for the object we found */
        listing = client.listObjects(reqParams);
        results.addAll( getFilenamesFromListing(listing, prefix));
        /* grab the common prefixes */
        results.addAll( getCommonPrefixFromListing(listing, prefix));
        
        
        /* loop until all the keys have been read */
        while(listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            
            results.addAll( getFilenamesFromListing(listing, prefix));
            /* grab the common prefixes */
            results.addAll( getCommonPrefixFromListing(listing, prefix));
        }

        if (results.size() == 0) {
            return null;
        }
        return results.toArray(new String[results.size()]);
    }

    @Override
    public CachedFile[] listFiles() {
        final ListObjectsRequest reqParams;
        final String prefix;
        final List<CachedFile> results = new ArrayList<CachedFile>(100);
        ObjectListing listing;
        final AmazonS3Client client = cache.getS3client();
        
        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(cache.getS3bucket());
        prefix = cache.getS3path(path) + DELIMITER;
        reqParams.setPrefix(prefix);
        reqParams.setDelimiter(DELIMITER);
        
        /* grab first set of keys for the object we found */
        listing = client.listObjects(reqParams);
        for (String filename : getFilenamesFromListing(listing, prefix)) {
            S3CachedFile cf = (S3CachedFile)CachedFile.create(prefix + filename);
            cf.type = TYPE_FILE;
            results.add(cf);
        }
        /* add the common prefixes */
        for (String dirname : getCommonPrefixFromListing(listing, prefix)) {
            S3CachedFile cf = (S3CachedFile)CachedFile.create(prefix + dirname);
            cf.type = TYPE_DIR;
            results.add(cf);
        }
        
        
        /* loop until all the keys have been read */
        while(listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            
            for (String filename : getFilenamesFromListing(listing, prefix)) {
                S3CachedFile cf = (S3CachedFile)CachedFile.create(prefix + filename);
                cf.type = TYPE_FILE;
                results.add(cf);
            }
            /* add the common prefixes */
            for (String dirname : getCommonPrefixFromListing(listing, prefix)) {
                S3CachedFile cf = (S3CachedFile)CachedFile.create(prefix + dirname);
                cf.type = TYPE_DIR;
                results.add(cf);
            }
        }

        if (results.size() == 0) {
            return null;
        }
        return results.toArray(new CachedFile[results.size()]);
    }

    @Override
    public File loadFile() throws IOException {
        try {
            return cache.getFile(path);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCached() {
        return cache.contains(path);
    }

    @Override
    public File loadDirectory() throws IOException {
        final ListObjectsRequest reqParams;
        final String prefix;
        ObjectListing listing;
        final AmazonS3Client client = cache.getS3client();
        
        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(cache.getS3bucket());
        prefix = cache.getS3path(path) + DELIMITER;
        reqParams.setPrefix(prefix);
        
        /* grab first set of keys for the object we found */
        listing = client.listObjects(reqParams);
        /* load all of the objects */
        for (String filename : getFilenamesFromListing(listing, prefix)) {
            try {
                cache.getFile(filename);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }
        
        
        /* loop until all the keys have been read */
        while(listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            
            /* load all of the objects */
            for (String filename : getFilenamesFromListing(listing, prefix)) {
                try {
                    cache.getFile(filename);
                } catch (ExecutionException e) {
                    throw new IOException(e);
                }
            }
        }

        final String localPath = cache.getLocalPath(path);
        return new File(localPath);
    }

    @Override
    public String getCanonicalPath() throws IOException {
        return path;
    }

    @Override
    public long length() {
        File f;
        try {
            f = cache.getFile(path);
        } catch (ExecutionException e) {
            return 0L;
        }
        return f.length();
    }

}
