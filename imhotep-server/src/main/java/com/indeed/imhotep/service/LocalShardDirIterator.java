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

package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.fs.DirectoryStreamFilters;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author kenh
 */

class LocalShardDirIterator implements Iterable<Pair<String, ShardDir>> {
    private static final Logger LOGGER = Logger.getLogger(LocalShardDirIterator.class);
    private final Path shardsPath;

    LocalShardDirIterator(final Path shardsPath) {
        this.shardsPath = shardsPath;
    }

    @Nonnull
    @Override
    public Iterator<Pair<String, ShardDir>> iterator() {
        try (DirectoryStream<Path> datasets = Files.newDirectoryStream(shardsPath, DirectoryStreamFilters.ONLY_DIRS)) {
            return FluentIterable.from(datasets).transformAndConcat(
                    new Function<Path, Iterable<Pair<String, ShardDir>>>() {
                        @Override
                        public Iterable<Pair<String, ShardDir>> apply(final Path dataSetPath) {
                            final String dataset = dataSetPath.getFileName().toString();
                            try (DirectoryStream<Path> shards = Files.newDirectoryStream(dataSetPath, DirectoryStreamFilters.ONLY_DIRS)) {
                                return FluentIterable.from(shards).transform(
                                        new Function<Path, Pair<String, ShardDir>>() {
                                            @Nullable
                                            @Override
                                            public Pair<String, ShardDir> apply(final Path shardPath) {
                                                final ShardDir shardDir = new ShardDir(shardPath);
                                                if (ShardTimeUtils.isValidShardId(shardDir.getId())) {
                                                    return Pair.of(dataset, shardDir);
                                                } else {
                                                    return null;
                                                }
                                            }
                                        }
                                ).filter(Predicates.<Pair<String,ShardDir>>notNull()).toList();
                            } catch (final IOException e) {
                                LOGGER.warn("Failed to scan for shard for dataset " + dataSetPath, e);
                                return Collections.emptyList();
                            }
                        }
                    }
            ).toList().iterator();
        } catch (final IOException e) {
            LOGGER.warn("Failed to scan for shards under directory " + shardsPath, e);
            return Collections.<Pair<String, ShardDir>>emptyList().iterator();
        }
    }

}
