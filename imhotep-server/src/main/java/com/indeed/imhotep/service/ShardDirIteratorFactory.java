package com.indeed.imhotep.service;

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
    private Properties shardFilterConfig;

    ShardDirIteratorFactory() {
        this(System.getProperty("imhotep.shard.filter.config.path"));
    }

    ShardDirIteratorFactory(@Nullable final String shardFilterConfigPath) {
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
    }

    ShardDirIterator get(final Path shardsPath) {
        if (shardFilterConfig != null) {
            return new FilteredShardDirIterator(
                    shardsPath,
                    FilteredShardDirIterator.Config.loadFromProperties(shardFilterConfig));
        } else {
            return new LocalShardDirIterator(shardsPath);
        }
    }
}
