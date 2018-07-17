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

import com.google.common.base.Throwables;
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
            throw Throwables.propagate(e);
        }
    }

    public Spliterator<Path> spliterator(){
        try {
            FileStatus[] fStatus = fs.listStatus(datasetsDir);
            return getDirs(fStatus).spliterator();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<Path> getDirs(FileStatus[] fStatus) {
        return Arrays.stream(fStatus).filter(FileStatus::isDirectory).map(FileStatus::getPath).collect(Collectors.toList());
    }
}
