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

package com.indeed.imhotep.shardmaster;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author kenh
 */

class DataSetScanner implements Iterable<Path> {
    private final Path datasetsDir;
    private final FileSystem fs;
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(DataSetScanner.class);

    DataSetScanner(final Path datasetsDir, final FileSystem fs) {
        this.datasetsDir = datasetsDir;
        this.fs = fs;
    }

    @Nonnull
    @Override
    public Iterator<Path> iterator() {
        try {
            FileStatus[] fStatus = fs.listStatus(datasetsDir);
            return getDirs(fStatus).iterator();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return new ArrayList<Path>().iterator();
        }
    }

    private boolean keepPossibleDir(FileStatus status) {
        return status.isDirectory();
    }

    public Spliterator<Path> spliterator(){
        try {
            FileStatus[] fStatus = fs.listStatus(datasetsDir);
            return getDirs(fStatus).spliterator();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return new ArrayList<Path>().spliterator();
        }
    }

    private List<Path> getDirs(FileStatus[] fStatus) {
        return Arrays.stream(fStatus).filter(this::keepPossibleDir).map(status -> status.getPath()).collect(Collectors.toList());
    }
}
