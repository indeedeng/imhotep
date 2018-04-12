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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class S3RemoteFileStore extends RemoteFileStore {
    private static final String DELIMITER = "/";

    private final String s3bucket;
    private final String s3prefix;
    private final AmazonS3Client client;

    private S3RemoteFileStore(final Map<String, ?> settings) {
        s3bucket = (String) settings.get("imhotep.fs.filestore.s3.bucket");
        s3prefix = Strings.nullToEmpty((String) settings.get("imhotep.fs.filestore.s3.prefix")).trim();
        final String s3key = (String) settings.get("imhotep.fs.filestore.s3.key");
        final String s3secret = (String) settings.get("imhotep.fs.filestore.s3.secret");
        final BasicAWSCredentials cred = new BasicAWSCredentials(s3key, s3secret);

        client = new AmazonS3Client(cred);
        final List<S3ObjectSummary> rootListing = getListing(s3prefix + DELIMITER, 1, true).getObjectSummaries();
        Preconditions.checkState(!rootListing.isEmpty(), "Could not find S3 common prefix " + s3prefix);
    }

    private ObjectListing getListing(final String s3path, final int maxResults, final boolean recursive) {
        final ListObjectsRequest reqParams = new ListObjectsRequest();
        reqParams.setBucketName(s3bucket);
        reqParams.setPrefix(s3path);
        if (maxResults > 0) {
            reqParams.setMaxKeys(maxResults);
        }
        if (!recursive) {
            reqParams.setDelimiter(DELIMITER);
        }

        return client.listObjects(reqParams);
    }

    private String getS3path(final RemoteCachingPath path) {
        final StringBuilder result = new StringBuilder(s3prefix);
        for (final Path component : path.normalize()) {
            if (result.length() > 0) {
                result.append(DELIMITER);
            }
            result.append(component.toString());
        }
        return result.toString();
    }

    @Override
    public List<RemoteFileAttributes> listDir(final RemoteCachingPath path) throws IOException {
        final String s3DirPath = getS3path(path) + DELIMITER;
        final List<RemoteFileAttributes> results = new ArrayList<>();

        ObjectListing listing = getListing(s3DirPath, -1, false);
        while (true) {
            // get files
            for (final S3ObjectSummary summary : listing.getObjectSummaries()) {
                final String filename = summary.getKey().substring(s3DirPath.length());
                results.add(new RemoteFileAttributes(path.resolve(filename), summary.getSize(), true));
            }
            // get directories
            for (final String prefix : listing.getCommonPrefixes()) {
                final String dirname = prefix.substring(s3DirPath.length(), prefix.length() - 1);
                results.add(new RemoteFileAttributes(path.resolve(dirname), -1, false));
            }

            if (listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
            } else {
                break;
            }
        }

        if (results.isEmpty()) {
            if (readFileInfo(path) != null) {
                throw new NotDirectoryException("Directory " + path + " not found");
            } else {
                throw new NoSuchFileException("Directory " + path + " not found");
            }
        }

        return results;
    }

    @Override
    public RemoteFileAttributes getRemoteAttributes(final RemoteCachingPath path) throws IOException {
        RemoteFileAttributes result = readFileInfo(path);
        if (result == null) {
            result = readDirInfo(path);
            if (result == null) {
                throw new NoSuchFileException(path + " not found");
            }
        }
        return result;
    }

    @Nullable
    private RemoteFileAttributes readFileInfo(final RemoteCachingPath path) {
        final String s3path = getS3path(path);
        final ObjectListing listing = getListing(s3path, 1, false);

        final List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        for (final S3ObjectSummary summary : summaries) {
            final String key = summary.getKey();
            if (key.equals(s3path)) {
                return new RemoteFileAttributes(path, summary.getSize(), true);
            }
        }
        return null;
    }

    @Nullable
    private RemoteFileAttributes readDirInfo(final RemoteCachingPath path) {
        final String s3DirPath = getS3path(path) + DELIMITER;

        final ObjectListing listing = getListing(s3DirPath, 1, true);

        final List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        for (final S3ObjectSummary summary : summaries) {
            final String key = summary.getKey();
            if (key.startsWith(s3DirPath)) {
                return new RemoteFileAttributes(path, -1, false);
            }
        }
        return null;
    }

    @Override
    public void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        final String s3path = getS3path(srcPath);

        try {
            client.getObject(new GetObjectRequest(s3bucket, s3path), destPath.toFile());
        } catch (final AmazonS3Exception e) {
            throw new IOException("Failed to download file " + srcPath, e);
        }
    }

    @Override
    public InputStream newInputStream(final RemoteCachingPath path, final long startOffset, final long length) throws
            IOException {
        final String s3path = getS3path(path);

        final GetObjectRequest request = new GetObjectRequest(s3bucket, s3path);
        if (length == -1) {
            request.setRange(startOffset, Long.MAX_VALUE);
        } else {
            request.setRange(startOffset, startOffset + length);
        }

        try {
            return client.getObject(request).getObjectContent();
        } catch (final AmazonS3Exception e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                throw new NoSuchFileException(path.toString(), s3path, e.toString());
            } else {
                throw new IOException("Failed to open input stream for " + path, e);
            }
        }
    }

    @Override
    public String name() {
        return s3bucket + ":" + s3prefix;
    }

    static class Factory implements RemoteFileStore.Factory {
        @Override
        public RemoteFileStore create(final Map<String, ?> configuration) {
            return new S3RemoteFileStore(configuration);
        }
    }
}
