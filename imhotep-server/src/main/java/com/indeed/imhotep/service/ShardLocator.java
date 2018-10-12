package com.indeed.imhotep.service;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

public interface ShardLocator {
    public static ShardLocator pathShardLocator(final Path rootDir) {
        return (dataset, shardName) -> Optional.of(rootDir.resolve(dataset).resolve(shardName));
    }

    public static ShardLocator combine(final ShardLocator... shardLocators) {
        return (dataset, shardName) -> Arrays.stream(shardLocators)
                .map(shardLocator -> shardLocator.locateShard(dataset, shardName))
                .filter(Optional::isPresent)
                .findFirst()
                .flatMap(Function.identity());
    }

    /**
     * @param dataset
     * @param shardName the directory name of the shard. see: {@link FlamdexInfo}.
     */
    Optional<Path> locateShard(final String dataset, final String shardName);
}
