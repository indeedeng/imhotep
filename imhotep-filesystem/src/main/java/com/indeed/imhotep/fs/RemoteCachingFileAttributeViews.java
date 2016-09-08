package com.indeed.imhotep.fs;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * @author kenh
 */

class RemoteCachingFileAttributeViews {
    static class Basic implements BasicFileAttributeView {
        private final RemoteCachingPath path;

        Basic(final RemoteCachingPath path) {
            this.path = path;
        }

        @Override
        public String name() {
            return "basic";
        }

        @Override
        public BasicFileAttributes readAttributes() throws IOException {
            return path.getFileSystem().getFileAttributes(path);
        }

        @Override
        public void setTimes(final FileTime lastModifiedTime, final FileTime lastAccessTime, final FileTime createTime) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    static class Imhotep extends Basic {
        Imhotep(final RemoteCachingPath path) {
            super(path);
        }

        @Override
        public String name() {
            return "imhotep";
        }
    }
}
