package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3RemoteFileSystem extends RemoteFileSystem {
    private String mountPoint;
    private RemoteFileSystemMounter mounter;
    private String s3bucket;
    private String s3prefix;
    private AmazonS3Client client;

    public S3RemoteFileSystem(Map<String,String> settings, 
                              RemoteFileSystem parent,
                              RemoteFileSystemMounter mounter) {
        final String s3key;
        final String s3secret;
        final BasicAWSCredentials cred;
        
        mountPoint = settings.get("mountpoint").trim();
        if (! mountPoint.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            mountPoint = mountPoint + DELIMITER;
        }
        mountPoint = mounter.getRootMountPoint() + mountPoint;
        mountPoint = mountPoint.replace("//", "/");
        
        this.mounter = mounter;
        
        s3bucket = settings.get("s3-bucket").trim();
        s3prefix = settings.get("s3-prefix");
        if (s3prefix != null) {
            s3prefix = RemoteFileSystem.cleanupPath(s3prefix.trim());
        }
        s3key = settings.get("s3-key").trim();
        s3secret = settings.get("s3-secret").trim();
        cred = new BasicAWSCredentials(s3key, s3secret);

        client = new AmazonS3Client(cred);
    }
    
    private String getS3path(String path) {
        
        if (s3prefix != null) {
            if (path.isEmpty()) {
                return s3prefix;
            } else {
                return s3prefix + DELIMITER + path;
            }
        }
        return path;
    }
    
    @Override
    public String getMountPoint() {
        return this.mountPoint;
    }
    
    @Override
    public File loadFile(String path) throws IOException {
        final File file;
        
        file = File.createTempFile("imhotep.s3.", ".cachedFile");
        copyFileInto(path, file);
        return file;
    }

    public String getRemotePath(String path) {
        String remotePath;
        final String mountPointNoDelim;
        
        mountPointNoDelim = mountPoint.substring(0, mountPoint.length() - DELIMITER.length());
        if (path.equals(mountPointNoDelim) || path.equals(mountPoint)) {
            return "";
        } else if (path.startsWith(mountPoint)) {
            remotePath = path.substring(mountPoint.length());
            remotePath = RemoteFileSystem.cleanupPath(remotePath);
            return remotePath;
        }
        
        throw new IllegalArgumentException("File is not located off the \"mount point\".");
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath);
        final ObjectMetadata metadata;
        
        try {
            metadata = client.getObject(new GetObjectRequest(s3bucket, s3path), localFile);
        } catch(AmazonS3Exception e) {
            throw new IOException(e);
        }
    }

    private ObjectMetadata getMetadata(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath);
        final ObjectMetadata metadata;
        
        try {
            metadata = client.getObjectMetadata(s3bucket, s3path);
        } catch(AmazonServiceException e) {
            return null;
        }
        
        return metadata;
    }
    
    private ObjectListing getListing(String s3path, int maxResults, boolean recursive) {
        final ListObjectsRequest reqParams;
        final ObjectListing listing;

        reqParams = new ListObjectsRequest();
        reqParams.setBucketName(s3bucket);
        reqParams.setPrefix(s3path);
        if (maxResults > 0) {
            reqParams.setMaxKeys(maxResults);
        }
        if (! recursive) {
            reqParams.setDelimiter(DELIMITER);
        }
        
        listing = client.listObjects(reqParams);
        
        return listing;
    }
    
    /*
     * Returns null if file not found
     * 
     */
    @Override
    public RemoteFileInfo stat(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath);
        final ObjectListing listing;
        final int type;
        
        listing = getListing(s3path, 1, true);
        
        if (listing.getObjectSummaries().size() == 0) {
            return null;
        }
        
        final String key;
        key = listing.getObjectSummaries().get(0).getKey();
        
        if (key.equals(s3path)) {
            type = RemoteFileInfo.TYPE_FILE;
        } else if (key.startsWith(s3path + DELIMITER)) {
            type = RemoteFileInfo.TYPE_DIR;
        } else {
            return null;
        }
       
        return new RemoteFileInfo(relativePath, type);
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
    public List<RemoteFileInfo> readDir(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath) + DELIMITER;
        final List<RemoteFileInfo> results = new ArrayList<RemoteFileInfo>(100);
        ObjectListing listing;
        
        /* grab first set of keys for the object we found */
        listing = getListing(s3path, -1, false);
        for (String filename : getFilenamesFromListing(listing, s3path)) {
            results.add(new RemoteFileInfo(filename, RemoteFileInfo.TYPE_FILE));
        }
        /* add the common prefixes */
        for (String dirname : getCommonPrefixFromListing(listing, s3path)) {
            results.add(new RemoteFileInfo(dirname, RemoteFileInfo.TYPE_DIR));
        }
        
        
        /* loop until all the keys have been read */
        while(listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            
            for (String filename : getFilenamesFromListing(listing, s3path)) {
                results.add(new RemoteFileInfo(filename, RemoteFileInfo.TYPE_FILE));
            }
            /* add the common prefixes */
            for (String dirname : getCommonPrefixFromListing(listing, s3path)) {
                results.add(new RemoteFileInfo(dirname, RemoteFileInfo.TYPE_DIR));
            }
        }

        if (results.size() == 0) {
            return null;
        }
        
        return results;
    }

    @Override
    public Map<String,File> loadDirectory(String fullPath, File location) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath) + DELIMITER;
        ObjectListing listing;
        final Map<String,File> results;
        
        results = new HashMap<String,File>(100);
        
        if (location == null) {
            location = File.createTempFile("s3", "remoteFile");
            location.delete();
            location.mkdir();
        }
        
        /* grab first set of keys for the object we found */
        listing = getListing(s3path, -1, true);
        /* load all of the objects */
        for (String filename : getFilenamesFromListing(listing, s3path)) {
            final String fname = joinPaths(fullPath, filename);
            final File localFile;

            localFile = new File(location, filename);
            /* create all the directories on the path to the file */
            localFile.getParentFile().mkdirs();

            /* download file */
            copyFileInto(fname, localFile);
            
            results.put(fname, localFile);
        }
        
        
        /* loop until all the keys have been read */
        while(listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);
            
            /* load all of the objects */
            for (String filename : getFilenamesFromListing(listing, s3path)) {
                final String fname = joinPaths(fullPath, filename);
                final File localFile;

                localFile = new File(location, filename);
                /* create all the directories on the path to the file */
                localFile.getParentFile().mkdirs();

                /* download file */
                copyFileInto(fname, localFile);
                
                results.put(fname, localFile);
            }
        }

        return results;
    }
    
    private String joinPaths(String dirPath, String filePath) {
        String path;
        
        if (dirPath.isEmpty()) {
            path = filePath;
        } else {
            path = dirPath + filePath;
        }
        
        return path;
    }

    @Override
    public InputStream getInputStreamForFile(String fullPath,
                                             long startOffset,
                                             long maxReadLength) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final String s3path = getS3path(relativePath) + DELIMITER;
        final S3Object s3obj;
        final S3ObjectInputStream is;
        final GetObjectRequest request;
        
        
        request = new GetObjectRequest(s3bucket, s3path);
        
        if (maxReadLength != -1) {
            request.setRange(startOffset, startOffset + maxReadLength);
        }

        try {
            s3obj = client.getObject(request);
            is = s3obj.getObjectContent();
            return new AutoAbortingS3InputStream(is, maxReadLength);
        } catch(AmazonS3Exception e) {
            throw new IOException(e);
        }
    }
    
    private static final class AutoAbortingS3InputStream extends FilterInputStream {
        private long amtRead;
        private long size;

        public AutoAbortingS3InputStream(S3ObjectInputStream baseIS,
                                         long maxReadLength) {
            super(baseIS);
            this.amtRead = 0;
            this.size = maxReadLength;
        }
        

        @Override
        public int read() throws IOException {
            return super.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int read = super.read(b, off, len);
            amtRead += read;
            return read;
        }

        @Override
        public long skip(long n) throws IOException {
            amtRead += n;
            return super.skip(n);
        }

        @Override
        public synchronized void mark(int readlimit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void reset() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            final long amtLeft;
            
            amtLeft = size - amtRead;
            if (amtLeft > 12000) {  // ~ 4 packets
                ((S3ObjectInputStream)in).abort();
            } else {
                in.close();
            }
        }
    }
}
