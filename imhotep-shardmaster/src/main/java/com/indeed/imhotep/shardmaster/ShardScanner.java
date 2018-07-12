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

import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * @author kenh
 */

class ShardScanner implements Iterable<ShardDir> {
    private final Path baseDir;
    private final FileSystem fs;
    private static final Logger LOGGER = Logger.getLogger(ShardScanner.class);

    ShardScanner(final Path baseDir, final  FileSystem fs) {
        this.baseDir = baseDir;
        this.fs = fs;
    }

    @Nonnull
    @Override
    public Iterator<ShardDir> iterator() {
        try {
            final FileStatus[] fileStatuses = fs.listStatus(baseDir, this::isValid);
            return Arrays.stream(fileStatuses).map(file -> new ShardDir(file.getPath())).collect(Collectors.toList()).iterator();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return new ArrayList<ShardDir>().iterator();
        }
    }

    private boolean isValid(Path shardPath) {
        ShardDir temp = new ShardDir(shardPath);
        String dataset = temp.getDataset();
        String id = temp.getId();
        return ShardTimeUtils.isValidShardId(id) &&
                !ShardData.getInstance().hasShard(dataset + "/" + temp.getName());
    }
}
