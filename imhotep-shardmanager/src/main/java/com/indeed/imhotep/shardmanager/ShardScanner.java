package com.indeed.imhotep.shardmanager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * @author kenh
 */

class ShardScanner implements Iterable<ShardDir> {
    private final RemoteCachingPath datasetDir;
    private final ShardFilter shardFilter;

    ShardScanner(final RemoteCachingPath datasetDir, final ShardFilter shardFilter) {
        this.datasetDir = datasetDir;
        this.shardFilter = shardFilter;
    }

    @Override
    public Iterator<ShardDir> iterator() {
        try {
            // hack to avoid an extra attribute lookup on each list entry
            final RemoteCachingFileSystemProvider fsProvider = (RemoteCachingFileSystemProvider) (((Path) datasetDir).getFileSystem().provider());

            return FluentIterable.from(fsProvider.newDirectoryStreamWithAttributes(datasetDir, DataSetScanner.ONLY_DIRS))
                    .filter(new Predicate<Path>() {
                        @Override
                        public boolean apply(final Path shardPath) {
                            final String dataset = shardPath.getParent().getFileName().toString();
                            final String shard = shardPath.getFileName().toString();
                            return shardFilter.accept(dataset, shard);
                        }
                    })
                    .transform(new Function<Path, ShardDir>() {
                        @Override
                        public ShardDir apply(final Path path) {
                            return new ShardDir(path);
                        }
                    }).iterator();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards from " + datasetDir, e);
        }
    }
}
