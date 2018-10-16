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

/**
 * Metadata about a file required for producing a directory listing
 */

public class RemoteFileListing {
    private final String name;
    private final boolean isFile;
    private final long size;

    /**
     * constructor for directory entity
     * @param dirName the directory path
     */
    public RemoteFileListing(final String dirName) {
        name = dirName;
        isFile = false;
        size = -1;
    }

    /**
     * constructor for file entity
     * @param fileMetadata the file metadata
     */
    public RemoteFileListing(final FileMetadata fileMetadata) {
        this.name = fileMetadata.getFilename();
        this.isFile = true;
        this.size = fileMetadata.getSize();
    }

    public RemoteFileListing(String name, boolean isFile, long size) {
        this.name = name;
        this.isFile = isFile;
        this.size = size;
    }

    public String getFilename() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public boolean isFile() {
        return isFile;
    }

    @Override
    public String toString() {
        return "RemoteFileListing{" +
                "name='" + name + '\'' +
                ", isFile=" + isFile +
                ", size=" + size +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteFileListing that = (RemoteFileListing) o;
        return isFile == that.isFile &&
                size == that.size &&
                Objects.equal(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, isFile, size);
    }
}
