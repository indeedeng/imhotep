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
package com.indeed.imhotep.io.caching.RemoteCaching;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.Map;

public class S3RemoteFileStore extends RemoteFileStore {
    private static final Logger log = Logger.getLogger(S3RemoteFileStore.class);

    private String s3bucket;
    private String s3prefix;
    private AmazonS3Client client;

    public S3RemoteFileStore(Map<String, String> settings) {
        final String s3key;
        final String s3secret;
        final BasicAWSCredentials cred;

        s3bucket = settings.get("s3-bucket");
        s3prefix = settings.get("s3-prefix");
        if (s3prefix != null) {
            s3prefix = s3prefix.trim();
        }
        s3key = settings.get("s3-key");
        s3secret = settings.get("s3-secret");
        cred = new BasicAWSCredentials(s3key, s3secret);

        client = new AmazonS3Client(cred);
    }

    private ArrayList<String> getCommonPrefixFromListing(ObjectListing listing, String prefix) {
        ArrayList<String> results = new ArrayList<>(100);

        for (String commonPrefix : listing.getCommonPrefixes()) {
            final String dirname;

            /* remove prefix and trailing delimiter */
            dirname = commonPrefix
                    .substring(prefix.length(), commonPrefix.length() - DELIMITER.length());
            if (dirname.length() == 0 || dirname.contains(DELIMITER)) {
                log.error("Error parsing S3 object prefix.  Prefix: " + commonPrefix);
                continue;
            }
            results.add(dirname);
        }

        return results;
    }

    private void getFilenamesFromListing(ObjectListing listing,
                                         String prefix,
                                         ArrayList<String> filenames,
                                         ArrayList<Long> sizes) {
        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            final String key = summary.getKey();
            final String filename;
            final long size;

            filename = key.substring(prefix.length());
            size = summary.getSize();
            if (filename.length() == 0 || filename.contains(DELIMITER)) {
                log.error("Error parsing S3 object Key.  Key: " + key);
                continue;
            }
            filenames.add(filename);
            sizes.add(size);
        }
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
        if (!recursive) {
            reqParams.setDelimiter(DELIMITER);
        }

        listing = client.listObjects(reqParams);

        return listing;
    }

    private ObjectMetadata getMetadata(RemoteCachingPath path) {
        final String s3path = getS3path(path);
        final ObjectMetadata metadata;

        try {
            metadata = client.getObjectMetadata(s3bucket, s3path);
        } catch (AmazonServiceException e) {
            return null;
        }

        return metadata;
    }

    private String getS3path(RemoteCachingPath path) {
        if (s3prefix != null && !s3prefix.isEmpty()) {
            return s3prefix + path.normalize().toString();
        }
        return path.toString();
    }

    private String getS3path(String pathStr) {
        if (s3prefix != null && !s3prefix.isEmpty()) {
            return s3prefix + pathStr;
        }
        return pathStr;
    }

    @Override
    public ArrayList<RemoteFileInfo> listDir(RemoteCachingPath path) {
        String s3path = getS3path(path);
        final ArrayList<RemoteFileInfo> results = new ArrayList<>(100);
        final ArrayList<String> filenames = new ArrayList<>(100);
        final ArrayList<Long> fileSizes = new ArrayList<>(100);
        ObjectListing listing;

        if (!s3path.isEmpty()) {
            s3path += DELIMITER;
        }

        /* grab first set of keys for the object we found */
        listing = getListing(s3path, -1, false);
        getFilenamesFromListing(listing, s3path, filenames, fileSizes);
        for (int i = 0; i < filenames.size(); i++) {
            results.add(new RemoteFileInfo(filenames.get(i), fileSizes.get(i), true));
        }
        /* add the common prefixes */
        for (String dirname : getCommonPrefixFromListing(listing, s3path)) {
            results.add(new RemoteFileInfo(dirname, -1, false));
        }


        /* loop until all the keys have been read */
        while (listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);

            getFilenamesFromListing(listing, s3path, filenames, fileSizes);
            for (int i = 0; i < filenames.size(); i++) {
                results.add(new RemoteFileInfo(filenames.get(i), fileSizes.get(i), true));
            }
            /* add the common prefixes */
            for (String dirname : getCommonPrefixFromListing(listing, s3path)) {
                results.add(new RemoteFileInfo(dirname, -1, false));
            }
        }

        if (results.size() == 0) {
            return null;
        }

        return results;
    }

    @Override
    public RemoteFileInfo readInfo(String pathStr) {
        final String s3path = getS3path(pathStr);

        final ObjectListing listing = getListing(s3path, 1, true);
        if (listing.getObjectSummaries().size() == 0) {
            return null;
        }

        final S3ObjectSummary summary = listing.getObjectSummaries().get(0);
        final String key = summary.getKey();
        if (key.equals(s3path)) {
            return new RemoteFileInfo(pathStr, summary.getSize(), true);
        } else if (s3path.isEmpty()) {
            return new RemoteFileInfo(pathStr, -1, false);
        } else if (key.startsWith(s3path + DELIMITER)) {
            return new RemoteFileInfo(pathStr, -1, false);
        } else {
            return null;
        }
    }

    @Override
    public void downloadFile(RemoteCachingPath path, Path tmpPath) throws IOException {
        final String s3path = getS3path(path);
        final ObjectMetadata metadata;

        try {
            metadata = client.getObject(new GetObjectRequest(s3bucket, s3path), tmpPath.toFile());
        } catch (AmazonS3Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputStream getInputStream(String path, long startOffset, long length) throws
                                                                                  IOException {
        final String s3path = (s3prefix != null && !s3prefix.isEmpty()) ? s3prefix + path : path;
        final S3Object s3obj;
        final GetObjectRequest request;

        request = new GetObjectRequest(s3bucket, s3path);
        if (length == -1) {
            request.setRange(startOffset, Long.MAX_VALUE);
        } else {
            request.setRange(startOffset, startOffset + length);
        }

        try {
            s3obj = client.getObject(request);
            return s3obj.getObjectContent();
        } catch (AmazonS3Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String name() {
        return "S3 File Store";
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
