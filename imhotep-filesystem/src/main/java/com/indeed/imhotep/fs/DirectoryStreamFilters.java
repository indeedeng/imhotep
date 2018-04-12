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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author kenh
 */

public class DirectoryStreamFilters {

    public static final DirectoryStream.Filter<Path> ONLY_DIRS = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(final Path entry) throws IOException {
            return Files.isDirectory(entry);
        }
    };

    public static final DirectoryStream.Filter<Path> ONLY_NON_DIRS = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(final Path entry) throws IOException {
            return Files.exists(entry) && !Files.isDirectory(entry);
        }
    };

    private DirectoryStreamFilters() {
    }
}
