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

import com.google.common.base.Objects;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

/**
 * @author kenh
 *
 * Tracks the metadata for a remote file or directory within a Sqar archive
 */

public class RemoteFileMetadata {
    private final FileMetadata fileMetadata;
    private final boolean isFile;
    private final long compressedSize;

    /**
     * constructor for directory entity
     * @param dirName the directory path
     */
    public RemoteFileMetadata(final String dirName) {
        fileMetadata = new FileMetadata(dirName, -1, -1, null, -1, SquallArchiveCompressor.NONE, null);
        isFile = false;
        compressedSize = -1;
    }

    /**
     * constructor for file entity
     * @param fileMetadata the file metadata
     * @param compressedSize the file's compressed size
     */
    public RemoteFileMetadata(final FileMetadata fileMetadata, final long compressedSize) {
        this.fileMetadata = fileMetadata;
        this.isFile = true;
        this.compressedSize = compressedSize;
    }

    public String getFilename() {
        return fileMetadata.getFilename();
    }

    public long getSize() {
        return fileMetadata.getSize();
    }

    public boolean isFile() {
        return isFile;
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }

    /**
     * @return returns the compressed file size. -1 if unknown
     */
    public long getCompressedSize() {
        return compressedSize;
    }

    public RemoteFileListing toListing() {
        return new RemoteFileListing(getFilename(), isFile, getSize());
    }

    @Override
    public String toString() {
        return "RemoteFileMetadata{" +
                "fileMetadata=" + fileMetadata +
                ", isFile=" + isFile +
                ", compressedSize=" + compressedSize +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RemoteFileMetadata)) {
            return false;
        }
        final RemoteFileMetadata that = (RemoteFileMetadata) o;
        return (isFile == that.isFile) &&
                (compressedSize == that.compressedSize) &&
                Objects.equal(fileMetadata, that.fileMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fileMetadata, isFile, compressedSize);
    }
}
