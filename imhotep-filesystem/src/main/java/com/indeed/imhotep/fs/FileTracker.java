package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Track collection of {@link RemoteCachingPath} by mirroring to a root {@link Path}
 *
 * @author darren
 */
class FileTracker {
    private final Path root;

    FileTracker(final Path root) {
        this.root = root;
    }

    void addDirectory(final RemoteCachingPath path) throws IOException {
        final Path trackingPath = createTrackingPath(path);
        Files.createDirectory(trackingPath);
    }

    private Path createTrackingPath(final RemoteCachingPath path) throws InvalidPathException {
        if (!path.isAbsolute()) {
            throw new InvalidPathException(path.toString(), "Path must be absolute");
        }
        return root.resolve(path.normalize().toString().substring(RemoteCachingPath.PATH_SEPARATOR_STR.length()));
    }

    Iterable<RemoteCachingPath> listDirectory(final RemoteCachingPath dir) throws IOException {
        try (final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(createTrackingPath(dir))) {
            return FluentIterable.from(directoryStream)
                    .transform(new Function<Path, RemoteCachingPath>() {
                        @Override
                        public RemoteCachingPath apply(final Path trackingPath) {
                            return new RemoteCachingPath(dir.getFileSystem(), dir.getRoot().resolve(trackingPath.relativize(root)).toString());
                        }
                    });
        }
    }

    ImhotepFileAttributes getAttributes(final RemoteCachingPath path) throws IOException {
        final Path trackingPath = createTrackingPath(path);
        final BasicFileAttributes basicAttrs = Files.readAttributes(trackingPath,
                BasicFileAttributes.class);

        return new ImhotepFileAttributes(basicAttrs.size(), basicAttrs.isDirectory());
    }

    boolean contains(final RemoteCachingPath path) {
        return Files.exists(createTrackingPath(path));
    }

    void addFile(final RemoteCachingPath path, final Path cachePath) throws IOException {
        Files.createLink(createTrackingPath(path), cachePath);
    }

    void removeFile(final RemoteCachingPath path) throws IOException {
        Files.delete(createTrackingPath(path));
    }
}
