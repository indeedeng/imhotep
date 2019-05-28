package com.indeed.imhotep.fs;

import com.google.common.collect.Iterables;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * util methods with nio callings
 * @author xweng
 */
public class NIOFileUtils {

    public static long sizeOfDirectory(final Path directory) throws IOException {
        final BasicFileAttributes dirAttrs = Files.readAttributes(directory, BasicFileAttributes.class);
        if (!dirAttrs.isDirectory()) {
            throw new IllegalArgumentException(directory + " is not a directory");
        }
        return Files.walk(directory).mapToLong(path -> {
            try {
                final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                return attributes.isDirectory() ? 0 : attributes.size();
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to get attributes for " + path, e);
            }
        }).sum();
    }

    /** Throws NotDirectoryException if it's not a directory */
    public static int fileCountOfDirectory(final Path directory) throws IOException {
        return Iterables.size(Files.newDirectoryStream(directory));
    }
}
