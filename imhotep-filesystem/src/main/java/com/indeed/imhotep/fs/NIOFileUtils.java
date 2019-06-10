package com.indeed.imhotep.fs;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.stream.Stream;

/**
 * util methods with nio callings
 * @author xweng
 */
class NIOFileUtils {

    /**
     * Compute the size of all files under the directory. If errors happen when reading file attributes, then just skip that file.
     * It's based on the assumption cache files are created/deleted frequently. So the method might return an approximate result.
     */
     static long sizeOfDirectory(final Path directory) throws IOException {
        final BasicFileAttributes dirAttrs = Files.readAttributes(directory, BasicFileAttributes.class);
        if (!dirAttrs.isDirectory()) {
            throw new IllegalArgumentException(directory + " is not a directory");
        }
        try (final Stream<Path> fileStream = Files.walk(directory)) {
            return fileStream.mapToLong(path -> {
                try {
                    final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                    return attributes.isDirectory() ? 0 : attributes.size();
                } catch (final IOException e) {
                    return 0;
                }
            }).sum();
        }
    }

    /** Throws NotDirectoryException if it's not a directory */
    static int fileCountOfDirectory(final Path directory) throws IOException {
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            return Iterables.size(dirStream);
        }
    }

    static Set<Path> listDirectory(final Path dirPath) throws IOException {
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(dirPath)) {
            return ImmutableSet.copyOf(dirStream);
        }
    }

    static Set<Path> listDirectory(final Path dirPath, final DirectoryStream.Filter<Path> filter) throws IOException {
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(dirPath, filter)) {
            return ImmutableSet.copyOf(dirStream);
        }
    }
}
