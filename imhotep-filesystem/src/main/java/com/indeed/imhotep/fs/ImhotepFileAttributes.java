package com.indeed.imhotep.fs;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * Created by darren on 12/21/15.
 */
public class ImhotepFileAttributes implements BasicFileAttributes {
    private final long size;
    private final boolean isFile;

    public ImhotepFileAttributes(long size, boolean isFile) {
        this.size = size;
        this.isFile = isFile;
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
        return this.isFile;
    }

    @Override
    public boolean isDirectory() {
        return ! this.isFile;
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
        return this.size;
    }

    @Override
    public Object fileKey() {
        return null;
    }
}
