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
package com.indeed.imhotep.fs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Strings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class S3RemoteFileStore extends RemoteFileStore {
    private static final String DELIMITER = "/";
    private static final Logger log = Logger.getLogger(S3RemoteFileStore.class);

    private final String s3bucket;
    private final String s3prefix;
    private final AmazonS3Client client;

    S3RemoteFileStore(final Map<String, String> settings) {
        final String s3key;
        final String s3secret;
        final BasicAWSCredentials cred;

        s3bucket = settings.get("s3-bucket");
        s3prefix = Strings.nullToEmpty(settings.get("s3-prefix")).trim();
        s3key = settings.get("s3-key");
        s3secret = settings.get("s3-secret");
        cred = new BasicAWSCredentials(s3key, s3secret);

        client = new AmazonS3Client(cred);
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

    private String getS3path(RemoteCachingPath path) {
        if (!s3prefix.isEmpty()) {
            return s3prefix + path.normalize().toString();
        }

        if (path.isAbsolute()) {
            return path.toString().substring(1);
        }
        return path.toString();
    }

    private String getS3path(String pathStr) {
        if (!s3prefix.isEmpty()) {
            return s3prefix + pathStr;
        }

        if (pathStr.charAt(0) == '/') {
            return pathStr.substring(1);
        }
        return pathStr;
    }

    @Override
    public List<RemoteFileInfo> listDir(RemoteCachingPath path) {
        final String s3path = getS3path(path);
        final String s3DirPath = (s3path.endsWith(DELIMITER)) ? s3path : s3path + DELIMITER;
        final List<RemoteFileInfo> results = new ArrayList<>(100);
        ObjectListing listing;

        /* grab first set of keys for the object we found */
        listing = getListing(s3DirPath, -1, false);
        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            /* remove prefix */
            final String filename = summary.getKey().substring(s3DirPath.length());
            results.add(new RemoteFileInfo(filename, summary.getSize(), true));
        }
        /* add the common prefixes */
        for (String prefix : listing.getCommonPrefixes()) {
                /* remove prefix and trailing delimiter */
            final String dirname = prefix.substring(s3DirPath.length(), prefix.length() - 1);
            results.add(new RemoteFileInfo(dirname, -1, false));
        }


        /* loop until all the keys have been read */
        while (listing.isTruncated()) {
            listing = client.listNextBatchOfObjects(listing);

            for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            /* remove prefix */
                final String filename = summary.getKey().substring(s3DirPath.length());
                results.add(new RemoteFileInfo(filename, summary.getSize(), true));
            }
            /* add the common prefixes */
            for (String prefix : listing.getCommonPrefixes()) {
                /* remove prefix and trailing delimiter */
                final String dirname = prefix.substring(s3DirPath.length(), prefix.length() - 1);
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
        final RemoteFileInfo result;

        result = readFileInfo(pathStr);
        if (result != null) {
            return result;
        } else {
            return readDirInfo(pathStr);
        }
    }

    @Override
    public RemoteFileInfo readInfo(String pathStr, boolean isFile) {
        if (isFile) {
            return readFileInfo(pathStr);
        } else {
            return readDirInfo(pathStr);
        }
    }

    private RemoteFileInfo readFileInfo(String pathStr) {
        final String s3path = getS3path(pathStr);
        final ObjectListing listing = getListing(s3path, 1, true);

        final List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.size() > 0) {
            final S3ObjectSummary summary = summaries.get(0);
            final String key = summary.getKey();
            if (key.equals(s3path)) {
                /* found a file matching the path */
                return new RemoteFileInfo(pathStr, summary.getSize(), true);
            }
        }
        return null;
    }

    private RemoteFileInfo readDirInfo(String pathStr) {
        final String s3path = getS3path(pathStr);
        final String s3DirPath = (s3path.endsWith(DELIMITER)) ? s3path : s3path + DELIMITER;

        final ObjectListing listing = getListing(s3DirPath, 1, true);

        final List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.size() > 0) {
            final S3ObjectSummary summary = summaries.get(0);
            final String key = summary.getKey();
            if (key.startsWith(s3DirPath)) {
                /* found a directory matching the path */
                return new RemoteFileInfo(pathStr, -1, false);
            }
        }
        return null;
    }

    @Override
    public void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final String s3path = getS3path(srcPath);
        final ObjectMetadata metadata;

        try {
            metadata = client.getObject(new GetObjectRequest(s3bucket, s3path), destPath.toFile());
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
        return s3bucket + ":" + s3prefix;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    public static class Builder implements RemoteFileStore.Builder {
        @Override
        public RemoteFileStore build(final Map<String, String> configuration) {
            return new S3RemoteFileStore(configuration);
        }
    }
}
