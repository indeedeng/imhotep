package com.indeed.imhotep.shardmanager;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.fs.RemoteCachingPath;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;

/**
 * @author kenh
 */

class DataSetScanner implements Iterable<RemoteCachingPath> {
    private final RemoteCachingPath datasetsDir;
    private final ShardFilter shardFilter;

    DataSetScanner(final RemoteCachingPath datasetsDir, final ShardFilter shardFilter) {
        this.datasetsDir = datasetsDir;
        this.shardFilter = shardFilter;
    }

    @Override
    public Iterator<RemoteCachingPath> iterator() {
        try {
            // hack to avoid an extra attribute lookup on each list entry
            final RemoteCachingFileSystemProvider fsProvider = (RemoteCachingFileSystemProvider) (((Path) datasetsDir).getFileSystem().provider());

            return FluentIterable.from(fsProvider.newDirectoryStreamWithAttributes(datasetsDir, ONLY_DIRS))
                    .filter(new Predicate<Path>() {
                        @Override
                        public boolean apply(final Path datasetPath) {
                            return shardFilter.accept(datasetPath.getFileName().toString());
                        }
                    }).iterator();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get datasets from " + datasetsDir, e);
        }
    }

    static final DirectoryStream.Filter<Pair<? extends Path, ? extends BasicFileAttributes>> ONLY_DIRS = new DirectoryStream.Filter<Pair<? extends Path, ? extends BasicFileAttributes>>() {
        @Override
        public boolean accept(final Pair<? extends Path, ? extends BasicFileAttributes> entry) throws IOException {
            return entry.getSecond().isDirectory();
        }
    };
}
