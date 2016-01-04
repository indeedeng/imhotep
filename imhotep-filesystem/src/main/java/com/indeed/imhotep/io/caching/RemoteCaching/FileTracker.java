package com.indeed.imhotep.io.caching.RemoteCaching;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.Iterator;

/**
 * Created by darren on 11/30/15.
 */
public class FileTracker {
    private final Path root;

    public FileTracker(Path root) {
        this.root = root;
    }


    public void addDirectory(RemoteCachingPath path, FileAttribute<?>[] attrs) throws IOException {
        final Path trackingPath = createTrackingPath(path);
        Files.createDirectory(trackingPath);
    }

    private Path createTrackingPath(RemoteCachingPath path) throws InvalidPathException {
        if (!path.isAbsolute()) {
            throw new InvalidPathException(path.toString(), "Path must be absolute", 0);
        }
        final String pathStr;
        pathStr = path.normalize().toString().substring(1);  /* PATH_SEPARATOR length = 1 */
        return root.resolve(pathStr);
    }

    public Iterator<RemoteCachingPath> listDirectory(final RemoteCachingPath dirPath) throws IOException {
        final Path trackingPath = createTrackingPath(dirPath);
        final Iterator<Path> iter = Files.newDirectoryStream(trackingPath).iterator();

        return new Iterator<RemoteCachingPath>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public RemoteCachingPath next() {
                final Path nextPath = iter.next();
                final Path relPath = nextPath.relativize(root);
                final Path absolutePath;

                absolutePath  = dirPath.getRoot().resolve(relPath.toString());
                return new RemoteCachingPath(dirPath.getFileSystem(), absolutePath.toString());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public ImhotepFileAttributes getAttributes(RemoteCachingPath remotePath) throws IOException {
        final Path trackingPath = createTrackingPath(remotePath);
        final BasicFileAttributes basicAttrs = Files.readAttributes(trackingPath,
                                                                    BasicFileAttributes.class);

        remotePath.initAttributes(basicAttrs.size(), !basicAttrs.isDirectory(), true);
        return remotePath.getAttributes();
    }

    public boolean contains(RemoteCachingPath path) {
        final Path trackingPath = createTrackingPath(path);
        return Files.exists(trackingPath);
    }

    public void addFile(RemoteCachingPath path, Path cachePath) throws IOException {
        final Path trackingPath = createTrackingPath(path);

        Files.createLink(trackingPath, cachePath);
    }

    public void removeFile(RemoteCachingPath path) throws IOException {
        final Path trackingPath = createTrackingPath(path);

        Files.delete(trackingPath);
    }

}
