package com.indeed.imhotep.service;

import java.nio.file.Path;
import java.util.Optional;

public interface DynamicShardLocator {
    public static final DynamicShardLocator NOTHING = (dataset, shardName) -> Optional.empty();

    Optional<Path> locateShard(final String dataset, final String shardName);
}
