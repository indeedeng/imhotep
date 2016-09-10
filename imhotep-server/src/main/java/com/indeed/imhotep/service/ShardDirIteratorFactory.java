package com.indeed.imhotep.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.indeed.imhotep.shardmanager.ShardManager;
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
    private final Logger LOGGER = Logger.getLogger(ShardDirIteratorFactory.class);
    private final WallClock wallClock;
    final Supplier<ShardManager> shardManager;
    private final String localHostname;
    private Properties shardFilterConfig;
    private boolean shardManagerEnabled;

    ShardDirIteratorFactory(final Supplier<ShardManager> shardManager, final String localHostname) {
        this(new DefaultWallClock(), shardManager, localHostname, System.getProperty("imhotep.shard.filter.config.file"), System.getProperty("imhotep.shard.shardmanager.enabled"));
    }

    @VisibleForTesting
    ShardDirIteratorFactory(final WallClock wallClock, final Supplier<ShardManager> shardManager,
                            final String localHostname,
                            @Nullable final String shardFilterConfigPath,
                            @Nullable final String shardManagerEnabled) {
        this.wallClock = wallClock;
        this.shardManager = shardManager;
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

        if ((shardManagerEnabled != null) && Boolean.parseBoolean(shardManagerEnabled)) {
            this.shardManagerEnabled = true;
        }
    }

    ShardDirIterator get(final Path shardsPath) {
        if (shardFilterConfig != null) {
            return new FilteredShardDirIterator(
                    wallClock,
                    shardsPath,
                    FilteredShardDirIterator.Config.loadFromProperties(shardFilterConfig));
        } else if (shardManagerEnabled) {
            return new ShardManagerShardDirIterator(shardManager,
                    localHostname);
        } else {
            return new LocalShardDirIterator(shardsPath);
        }
    }
}
