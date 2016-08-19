package com.indeed.imhotep.service;

import com.indeed.util.core.Pair;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author kenh
 * iterates over shards for a given dataset
 */
interface ShardDirIterator extends Iterable<Pair<String, ShardDir>> {
    DirectoryStream.Filter<Path> ONLY_DIRS = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(final Path entry) throws IOException {
            return Files.exists(entry) && Files.isDirectory(entry);
        }
    };
}
