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

import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

/**
 * @author jsgroth
 */
public class FileMetadata {
    private String filename;
    private final long size;
    private long compressedSize = -1;
    private final long timestamp;
    private final String checksum;
    private final long checksumHi;
    private final long checksumLow;
    private final long startOffset;
    private final SquallArchiveCompressor compressor;
    private final boolean isFile;
    private String archiveFilename;

    public FileMetadata(String filename,
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
        this.checksumHi = parseUnsignedLongHex(checksum.substring(0,16));
        this.checksumLow = parseUnsignedLongHex(checksum.substring(16));
        this.startOffset = startOffset;
        this.compressor = compressor;
        this.archiveFilename = archiveFilename;
        this.isFile = true;
    }

    private static long parseUnsignedLongHex(String numStr) {
        final long mostSigBits;
        final long leastSigBits;
        final int len;

        if (numStr.length() > 8) {
            // parse high order bits
            len = numStr.length() - 8;
            mostSigBits = Long.parseLong(numStr.substring(0, len), 16);
        } else {
            len = 0;
            mostSigBits = 0;
        }
        leastSigBits = Long.parseLong(numStr.substring(len), 16);
        return (mostSigBits << 32) | leastSigBits;
    }

    public FileMetadata(String filename,
                        long unpackedSize,
                        long packedSize,
                        long timestamp,
                        long sigHi,
                        long sigLow,
                        long archiveOffset,
                        SquallArchiveCompressor compressor,
                        String archiveFilename,
                        boolean isFile) {
        this.filename = filename;
        this.size = unpackedSize;
        this.timestamp = timestamp;
        this.checksum = String.format("%016x%016x", sigHi, sigLow);
        this.checksumHi = sigHi;
        this.checksumLow = sigLow;
        this.startOffset = archiveOffset;
        this.compressor = compressor;
        this.archiveFilename = archiveFilename;
        this.compressedSize = packedSize;
        this.isFile = isFile;
    }

    public FileMetadata(String dirName, boolean isFile) {
        if (isFile) {
            throw new IllegalArgumentException("This constructor is only for directories.");
        }
        this.isFile = false;
        this.filename = dirName;
        this.size = -1;
        this.timestamp = -1;
        this.checksum = null;
        this.checksumHi = -1;
        this.checksumLow = -1;
        this.startOffset = -1;
        this.compressor = SquallArchiveCompressor.NONE;
        this.archiveFilename = null;
        this.compressedSize = -1;
    }

    public String getFilename() {
        return filename;
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

    public long getChecksumHi() {
        return checksumHi;
    }

    public long getChecksumLow() {
        return checksumLow;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public SquallArchiveCompressor getCompressor() {
        return compressor;
    }

    public String getArchiveFilename() {
        return archiveFilename;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileMetadata that = (FileMetadata) o;

        if (size != that.size) return false;
        if (startOffset != that.startOffset) return false;
        if (timestamp != that.timestamp) return false;
        if (archiveFilename != null ? !archiveFilename.equals(that.archiveFilename) : that.archiveFilename != null) {
            return false;
        }
        if (checksum != null ? !checksum.equals(that.checksum) : that.checksum != null) return false;
        if (compressor != that.compressor) return false;
        if (filename != null ? !filename.equals(that.filename) : that.filename != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = filename != null ? filename.hashCode() : 0;
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (checksum != null ? checksum.hashCode() : 0);
        result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
        result = 31 * result + (compressor != null ? compressor.hashCode() : 0);
        result = 31 * result + (archiveFilename != null ? archiveFilename.hashCode() : 0);
        return result;
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
                '}';
    }

    public void setFilename(String str) {
        this.filename = str;
    }

    public void setArchiveFilename(String str) {
        this.archiveFilename = str;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(long compressedSize) {
        this.compressedSize = compressedSize;
    }

    public boolean isFile() {
        return isFile;
    }
}
