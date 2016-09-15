package com.indeed.imhotep.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.WallClock;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @author kenh
 */

class ShardDirIteratorFactory {
    private static final Logger LOGGER = Logger.getLogger(ShardDirIteratorFactory.class);
    private final WallClock wallClock;
    private final Supplier<ShardMaster> shardMasterSupplier;
    private final String localHostname;
    private Properties shardFilterConfig;
    private final boolean shardMasterEnabled;

    ShardDirIteratorFactory(final Supplier<ShardMaster> shardMasterSupplier, final String localHostname) {
        this(new DefaultWallClock(), shardMasterSupplier, localHostname, System.getProperty("imhotep.shard.filter.config.file"), System.getProperty("imhotep.shard.shardmaster.enabled"));
    }

    @VisibleForTesting
    ShardDirIteratorFactory(final WallClock wallClock, final Supplier<ShardMaster> shardMasterSupplier,
                            final String localHostname,
                            @Nullable final String shardFilterConfigPath,
                            @Nullable final String shardMasterEnabled) {
        this.wallClock = wallClock;
        this.shardMasterSupplier = shardMasterSupplier;
        this.localHostname = localHostname;
        if (shardFilterConfigPath == null) {
            shardFilterConfig = null;
        } else {
            try (InputStream is = Files.newInputStream(Paths.get(shardFilterConfigPath))) {
                shardFilterConfig = new Properties();
                shardFilterConfig.load(is);
            } catch (final IOException e) {
                LOGGER.warn("Failed to parse shard filter configuration " + shardFilterConfigPath, e);
                shardFilterConfig = null;
            }
        }

        this.shardMasterEnabled = (shardMasterEnabled != null) && Boolean.parseBoolean(shardMasterEnabled);
    }

    ShardDirIterator get(final Path shardsPath) {
        if (shardFilterConfig != null) {
            return new FilteredShardDirIterator(
                    wallClock,
                    shardsPath,
                    FilteredShardDirIterator.Config.loadFromProperties(shardFilterConfig));
        } else if (shardMasterEnabled) {
            return new ShardMasterShardDirIterator(shardMasterSupplier,
                    localHostname);
        } else {
            return new LocalShardDirIterator(shardsPath);
        }
    }
}
