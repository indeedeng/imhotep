package com.indeed.imhotep.fs;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * @author darren
 */
class ImhotepFileAttributes implements BasicFileAttributes {
    private final long size;
    private final boolean isDirectory;

    ImhotepFileAttributes(final long size, final boolean isDirectory) {
        this.size = size;
        this.isDirectory = isDirectory;
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.fromMillis(0);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.fromMillis(0);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.fromMillis(0);
    }

    @Override
    public boolean isRegularFile() {
        return true;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Object fileKey() {
        return null;
    }
}
