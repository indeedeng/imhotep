package com.indeed.imhotep.service;

import com.google.common.collect.ComparisonChain;
import com.indeed.imhotep.ShardDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public interface ShardLocator {
    public static ShardLocator appendingSQARShardLocator(final Path rootDir) {
        return (dataset, shardName) -> {
            final String archiveName;
            if (shardName.endsWith(".sqar")) {
                archiveName = shardName;
            } else {
                archiveName = shardName + ".sqar";
            }
            final Path shardPath = rootDir.resolve(dataset).resolve(archiveName);
            if (Files.exists(shardPath)) {
                return Optional.of(shardPath);
            } else {
                return Optional.empty();
            }
        };
    }

    public static ShardLocator pathShardLocator(final Path rootDir) {
        return (dataset, shardName) -> {
            final Path shardPath = rootDir.resolve(dataset).resolve(shardName);
            if (Files.exists(shardPath)) {
                return Optional.of(shardPath);
            } else {
                return Optional.empty();
            }
        };
    }

    /**
     * Call multiple shard locators, and return a shard that has the latest version.
     * Nulls in the arguments are just ignored.
     */
    public static ShardLocator combine(final ShardLocator... shardLocators) {
        return new CombinedShardLocator(
                Arrays.stream(shardLocators)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
        );
    }

    /**
     * @param dataset
     * @param shardName the directory name of the shard. see: {@link FlamdexInfo}.
     */
    Optional<Path> locateShard(final String dataset, final String shardName);

    static class CombinedShardLocator implements ShardLocator {
        private static final Comparator<ShardDir> SHARD_DIR_COMPARATOR = (lhs, rhs) -> {
            return ComparisonChain.start()
                    .compare(lhs.getVersion(), rhs.getVersion())
                    .compare(lhs.getName(), rhs.getName())
                    .compare(lhs.getIndexDir(), rhs.getIndexDir(), Comparator.comparing(Path::toString))
                    .result();
        };

        final List<ShardLocator> shardLocators;

        private CombinedShardLocator(final List<ShardLocator> shardLocators) {
            this.shardLocators = shardLocators;
        }

        @Override
        public Optional<Path> locateShard(final String dataset, final String shardName) {
            return shardLocators.stream()
                    .map(shardLocator -> shardLocator.locateShard(dataset, shardName))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(ShardDir::new)
                    .max(SHARD_DIR_COMPARATOR)
                    .map(ShardDir::getIndexDir);
        }
    }
}
