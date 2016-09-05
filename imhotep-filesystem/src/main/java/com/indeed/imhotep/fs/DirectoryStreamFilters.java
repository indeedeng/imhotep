package com.indeed.imhotep.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author kenh
 */

public class DirectoryStreamFilters {

    public static final DirectoryStream.Filter<Path> ONLY_DIRS = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(final Path entry) throws IOException {
            return Files.isDirectory(entry);
        }
    };

    public static final DirectoryStream.Filter<Path> ONLY_NON_DIRS = new DirectoryStream.Filter<Path>() {
        @Override
        public boolean accept(final Path entry) throws IOException {
            return Files.exists(entry) && !Files.isDirectory(entry);
        }
    };

    private DirectoryStreamFilters() {
    }
}
