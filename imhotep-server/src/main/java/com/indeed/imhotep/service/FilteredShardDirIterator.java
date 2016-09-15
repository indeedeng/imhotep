package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import com.indeed.imhotep.fs.DirectoryStreamFilters;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author kenh
 */

class FilteredShardDirIterator implements ShardDirIterator {
    private static final Logger LOGGER = Logger.getLogger(FilteredShardDirIterator.class);
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);
    private final WallClock wallClock;
    private final Path shardsPath;
    private final Config config;

    public FilteredShardDirIterator(final WallClock wallClock, final Path shardsPath, final Config config) {
        this.wallClock = wallClock;
        this.shardsPath = shardsPath;
        this.config = config;
    }

    @Override
    public Iterator<Pair<String, ShardDir>> iterator() {
        final DateTime now = new DateTime(wallClock.currentTimeMillis(), TIME_ZONE);
        try {
            return FluentIterable.from(Files.newDirectoryStream(shardsPath, DirectoryStreamFilters.ONLY_DIRS)).transformAndConcat(
                    new Function<Path, Iterable<Pair<String, ShardDir>>>() {
                        @Override
                        public Iterable<Pair<String, ShardDir>> apply(final Path dataSetPath) {
                            final String dataset = dataSetPath.getFileName().toString();
                            if (config.includeDataSet(dataset)) {
                                try {
                                    return FluentIterable.from(Files.newDirectoryStream(dataSetPath, DirectoryStreamFilters.ONLY_DIRS)).transform(
                                            new Function<Path, Pair<String, ShardDir>>() {
                                                @Override
                                                public Pair<String, ShardDir> apply(final Path shardPath) {
                                                    final ShardDir shardDir = new ShardDir(shardPath);
                                                    if (config.includeShard(now, dataset, shardDir)) {
                                                        return Pair.of(dataset, shardDir);
                                                    } else {
                                                        return null;
                                                    }
                                                }
                                            }
                                    ).filter(Predicates.notNull());
                                } catch (final IOException e) {
                                    LOGGER.warn("Failed to scan for shard for dataset " + dataSetPath, e);
                                }
                            }
                            return Collections.emptyList();
                        }
                    }
            ).iterator();
        } catch (final IOException e) {
            LOGGER.warn("Failed to scan for shards under directory " + shardsPath, e);
            return Collections.<Pair<String, ShardDir>>emptyList().iterator();
        }
    }

    static class Config {
        private final Map<String, Period> dataSetInterval;

        Config(final Map<String, Period> dataSetInterval) {
            this.dataSetInterval = dataSetInterval;
        }

        boolean includeDataSet(final String dataset) {
            return dataSetInterval.containsKey(dataset);
        }

        boolean includeShard(final DateTime now, final String dataset, final ShardDir shardDir) {
            final Period period = dataSetInterval.get(dataset);
            if (period != null) {
                final DateTime shardTime = ShardTimeUtils.parseStart(shardDir.getId());
                final DateTime threshold = now.minus(period);
                return threshold.isBefore(shardTime) || threshold.isEqual(shardTime);
            }
            return false;
        }

        static final String PROPERTY_PREFIX = "imhotep.shard.filter.include.";

        static Config loadFromProperties(final Properties properties) {
            final Map<String, Period> dataSetInterval = new HashMap<>();

            for (final Map.Entry entry : properties.entrySet()) {
                final String key = (String) entry.getKey();
                final String value = (String) entry.getValue();
                if (key.startsWith(PROPERTY_PREFIX)) {
                    final String dataset = key.substring(PROPERTY_PREFIX.length());
                    final Period period = Period.parse(value);
                    dataSetInterval.put(dataset, period);
                }
            }
            return new Config(dataSetInterval);
        }
    }
}
