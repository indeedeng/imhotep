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
        private static final Comparator<Path> SHARD_VERSION_COMPARATOR = new Comparator<Path>() {
            @Override
            public int compare(final Path lhs, final Path rhs) {
                final ShardDir lhsDir = new ShardDir(lhs);
                final ShardDir rhsDir = new ShardDir(rhs);
                return ComparisonChain.start()
                        .compare(lhsDir.getVersion(), rhsDir.getVersion())
                        .compare(lhsDir.getName(), rhsDir.getName())
                        .compare(lhs, rhs)
                        .result();
            }

            ;
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
                    .max(SHARD_VERSION_COMPARATOR);
        }
    }
}
