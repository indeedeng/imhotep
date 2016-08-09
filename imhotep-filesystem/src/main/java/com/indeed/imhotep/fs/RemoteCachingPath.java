package com.indeed.imhotep.fs;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author darren
 */
public class RemoteCachingPath implements Path, Serializable {
    private static final Logger log = Logger.getLogger(RemoteCachingPath.class);
    public static final char PATH_SEPARATOR = '/';
    public static final String PATH_SEPARATOR_STR = "/";

    private final RemoteCachingFileSystem fileSystem;
    private final String path;
    private final int[] offsets;
    private final String normalizedPath;
    private boolean attrLocalOnly = false;

    RemoteCachingPath(final RemoteCachingFileSystem fs, final String path) {
        this.fileSystem = fs;
        this.path = cleanPath(path);
        this.offsets = calcOffsets(this.path);
        this.normalizedPath = FilenameUtils.normalizeNoEndSeparator(path);
    }

    // TODO: will address this later
    RemoteCachingPath(final RemoteCachingFileSystem2 fs, final String path) {
        this.fileSystem = null;
        this.path = cleanPath(path);
        this.offsets = calcOffsets(this.path);
        this.normalizedPath = FilenameUtils.normalizeNoEndSeparator(path);
    }

    private static String cleanPath(String path) {
        if (path.charAt(path.length() - 1) == PATH_SEPARATOR) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static int[] calcOffsets(String path) {
        final int[] tmpArr = new int[path.length() + 1];
        int count = 0;

        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) != PATH_SEPARATOR) {
                tmpArr[count] = i;
                count++;
                while (i < path.length() && path.charAt(i) != PATH_SEPARATOR) {
                    i++;
                }
            }
        }
        return Arrays.copyOf(tmpArr, count);
    }

    @Override
    public RemoteCachingFileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return ((!path.isEmpty()) && (path.charAt(0) == PATH_SEPARATOR));
    }

    @Override
    public RemoteCachingPath getRoot() {
        if (this.isAbsolute()) {
            return new RemoteCachingPath(fileSystem, Character.toString(PATH_SEPARATOR));
        } else {
            return null;
        }
    }

    @Override
    public Path getFileName() {
        final int start = offsets[offsets.length - 2];
        final int end = offsets[offsets.length - 1];
        return new RemoteCachingPath(fileSystem, path.substring(start, end));
    }

    @Override
    public Path getParent() {
        return subpath(0, offsets.length - 1);
    }

    @Override
    public int getNameCount() {
        return offsets.length;
    }

    @Override
    public Path getName(int index) {
        return subpath(index, index + 1);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        if ((beginIndex < 0) ||
                (beginIndex >= offsets.length) ||
                (endIndex > offsets.length) ||
                (beginIndex >= endIndex)) {
            throw new IllegalArgumentException();
        }

        // starting and ending offsets
        final int start = offsets[beginIndex];
        final int end;
        if (endIndex == offsets.length) {
            end = path.length();
        } else {
            end = offsets[endIndex] - 1;
        }
        return new RemoteCachingPath(fileSystem, path.substring(start, end));
    }

    @Override
    public boolean startsWith(Path other) {
        return false;
    }

    @Override
    public boolean startsWith(String other) {
        return false;
    }

    @Override
    public boolean endsWith(Path other) {
        return false;
    }

    @Override
    public boolean endsWith(String other) {
        return false;
    }

    @Override
    public Path normalize() {
        if (normalizedPath.equals(path)) {
            return this;
        }
        return new RemoteCachingPath(fileSystem, normalizedPath);
    }

    @Override
    public Path resolve(Path other) {
        final RemoteCachingPath otherPath;

        otherPath = RemoteCachingFileSystemProvider.toRCP(other);
        if (otherPath.isAbsolute()) {
            return other;
        }

        final String newPath;
        if (this.path.endsWith(PATH_SEPARATOR_STR)) {
            newPath = this.path + PATH_SEPARATOR_STR + otherPath.path;
        } else {
            newPath = this.path + otherPath.path;
        }
        return new RemoteCachingPath(this.fileSystem, newPath);
    }

    @Override
    public Path resolve(String other) {
        final RemoteCachingPath otherPath = new RemoteCachingPath(fileSystem, other);
        if (otherPath.isAbsolute()) {
            return otherPath;
        }

        final String newPath;
        if (this.path.endsWith(PATH_SEPARATOR_STR)) {
            newPath = this.path + PATH_SEPARATOR_STR + otherPath.path;
        } else {
            newPath = this.path + otherPath.path;
        }
        return new RemoteCachingPath(this.fileSystem, newPath);
    }

    @Override
    public Path resolveSibling(Path other) {
        if (other == null) {
            throw new NullPointerException();
        }

        final Path parent = getParent();
        return (parent == null) ? other : parent.resolve(other);
    }

    @Override
    public Path resolveSibling(String other) {
        if (other == null) {
            throw new NullPointerException();
        }

        final Path parent = getParent();
        return (parent == null) ?
                new RemoteCachingPath(this.fileSystem, other) :
                parent.resolve(other);
    }

    @Override
    public Path relativize(Path other) {
        // TODO: make work in the general case
        final RemoteCachingPath o = RemoteCachingFileSystemProvider.toRCP(other);

        if (this.getRoot().equals(o)) {
            if (this.equals(o)) {
                /* both are root */
                return new RemoteCachingPath(this.fileSystem, ".");
            }
            return new RemoteCachingPath(this.fileSystem, makeRelative(this.path));
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public URI toUri() {
        try {
            return new URI(this.fileSystem.provider().getScheme(), null, path, null, null);
        } catch (URISyntaxException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    @Override
    public Path toAbsolutePath() {
        return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File toFile() {
        try {
            return fileSystem.getCacheFile(this);
        } catch (IOException e) {
            log.error("Could not load Path " + path, e);
            return new File("/INVALID/INVALID/INVALID");
        }
    }

    @Override
    public WatchKey register(WatchService watcher,
                             WatchEvent.Kind<?>[] events,
                             WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws
            IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        return null;
    }

    @Override
    public int compareTo(Path other) {
        final RemoteCachingPath otherPath = RemoteCachingFileSystemProvider.toRCP(other);
        return this.path.compareTo(otherPath.path);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RemoteCachingPath)) {
            return false;
        }

        final RemoteCachingPath paths = (RemoteCachingPath) o;

        if (!fileSystem.equals(paths.fileSystem)) {
            return false;
        }
        return path.equals(paths.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    public String getIndexPath() {
        if (offsets.length < 1) {
            return null;
        }
        return makeRelative(subpath(0, 1).toString());
    }

    public String getShardPath() {
        if (offsets.length < 2) {
            return null;
        }
        return makeRelative(subpath(0, 2).toString());
    }

    public String getFilePath() {
        if (offsets.length < 3) {
            return null;
        }
        return makeRelative(subpath(2, getNameCount()).toString());
    }

    public ImhotepPathType getType() {
        switch (offsets.length) {
            case 0:
                return ImhotepPathType.ROOT;
            case 1:
                return ImhotepPathType.INDEX;
            case 2:
                return ImhotepPathType.SHARD;
            default:
                return ImhotepPathType.FILE;
        }
    }

    boolean isLocalOnly() {
        return attrLocalOnly;
    }

    void setLocalOnly(boolean localOnly) {
        attrLocalOnly = localOnly;
    }

    private static String makeRelative(String path) {
        if (path.startsWith(PATH_SEPARATOR_STR)) {
            return path.substring(1);
        } else {
            return path;
        }
    }

    private Object writeReplace() throws ObjectStreamException {
        return new PathProxy(this);
    }

    @Override
    public String toString() {
        return normalizedPath;
    }
}
