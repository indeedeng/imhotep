package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.writer.FlamdexDocument;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author michihiko
 */

class ShardCommitterWithValidate extends DynamicFlamdexShardCommitter {
    private final Set<FlamdexDocument> naiveResult;
    private final List<Path> shardDirectories;

    ShardCommitterWithValidate(
            @Nonnull final Set<FlamdexDocument> naiveResult,
            @Nonnull final Path datasetDirectory,
            @Nonnull final String shardDirectoryPrefix,
            @Nullable final Path latestShardDirectory
    ) throws IOException {
        super(datasetDirectory, shardDirectoryPrefix, latestShardDirectory);
        this.naiveResult = naiveResult;
        this.shardDirectories = new ArrayList<>();
    }

    @Override
    public Path commit(@Nonnull final Long version) throws IOException {
        final Path shardDirectory = super.commit(version);
        shardDirectories.add(shardDirectory);
        try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(shardDirectory)) {
            DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
        }
        return shardDirectory;
    }

    public List<Path> getShardDirectories() {
        return shardDirectories;
    }
}
