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
 package com.indeed.imhotep.archive;

import com.google.common.base.Objects;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

/**
 * @author jsgroth
 */
public class FileMetadata {
    private final String filename;
    private final long size;
    private final long timestamp;
    private final String checksum;
    private final long startOffset;
    private final SquallArchiveCompressor compressor;
    private final String archiveFilename;

    public FileMetadata(final String filename,
                        long size,
                        long timestamp,
                        String checksum,
                        long startOffset,
                        SquallArchiveCompressor compressor,
                        String archiveFilename) {
        this.filename = filename;
        this.size = size;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.startOffset = startOffset;
        this.compressor = compressor;
        this.archiveFilename = archiveFilename;
    }

    public String getFilename() {
        return filename;
    }

    @Override
    public String toString() {
        return "FileMetadata{" +
                "filename='" + filename + '\'' +
                ", size=" + size +
                ", timestamp=" + timestamp +
                ", checksum='" + checksum + '\'' +
                ", startOffset=" + startOffset +
                ", compressor=" + compressor +
                ", archiveFilename='" + archiveFilename + '\'' +
                '}';
    }

    public long getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public SquallArchiveCompressor getCompressor() {
        return compressor;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileMetadata)) {
            return false;
        }
        final FileMetadata that = (FileMetadata) o;
        return size == that.size &&
                timestamp == that.timestamp &&
                startOffset == that.startOffset &&
                Objects.equal(filename, that.filename) &&
                Objects.equal(checksum, that.checksum) &&
                compressor == that.compressor &&
                Objects.equal(archiveFilename, that.archiveFilename);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(filename, size, timestamp, checksum, startOffset, compressor, archiveFilename);
    }

    public String getArchiveFilename() {
        return archiveFilename;
    }
}
