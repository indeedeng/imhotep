package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.util.core.time.DefaultWallClock;

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

class IndexCommitterWithValidate extends DynamicFlamdexIndexCommitter {
    private final Set<FlamdexDocument> naiveResult;
    private final List<Path> indexDirectories;

    IndexCommitterWithValidate(
            @Nonnull final Set<FlamdexDocument> naiveResult,
            @Nonnull final Path datasetDirectory,
            @Nonnull final String indexDirectoryPrefix,
            @Nullable final Path latestIndexDirectory
    ) throws IOException {
        super(datasetDirectory, indexDirectoryPrefix, latestIndexDirectory, new DefaultWallClock());
        this.naiveResult = naiveResult;
        this.indexDirectories = new ArrayList<>();
    }

    @Nonnull
    @Override
    protected Path commit(@Nonnull final Long version) throws IOException {
        final Path indexDirectory = super.commit(version);
        indexDirectories.add(indexDirectory);
        try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
            DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
        }
        return indexDirectory;
    }

    public List<Path> getIndexDirectories() {
        return indexDirectories;
    }
}
