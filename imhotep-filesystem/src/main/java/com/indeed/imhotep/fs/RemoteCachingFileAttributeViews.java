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
