/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.fs;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * @author darren
 */
public class RemoteCachingPath implements Path, Serializable {
    static final String PATH_SEPARATOR_STR = "/";

    private final RemoteCachingFileSystem fileSystem;
    private final String path;
    private final int[] nameOffsets;
    private final String normalizedPath;

    RemoteCachingPath(final RemoteCachingFileSystem fs, final String path) {
        fileSystem = fs;
        this.path = cleanPath(path);
        nameOffsets = calcNameOffsets(this.path);
        // TODO: Technically incorrect if the separate is not System dependent
        normalizedPath = FilenameUtils.normalizeNoEndSeparator(path);
        Preconditions.checkArgument(normalizedPath != null, "Unsupported path " + path);
    }

    private static String cleanPath(final String pathStr) {
        if (!PATH_SEPARATOR_STR.equals(pathStr) && pathStr.endsWith(PATH_SEPARATOR_STR)) {
            return pathStr.substring(0, pathStr.length() - PATH_SEPARATOR_STR.length());
        }
        return pathStr;
    }

    private static int[] calcNameOffsets(final String pathStr) {
        int index = 0;
        int count = 0;
        final int[] tmp = new int[pathStr.length()];

        if (!pathStr.isEmpty() && !pathStr.startsWith(PATH_SEPARATOR_STR)) {
            // in case of relative path
            tmp[count] = 0;
            count++;
        }

        while (index < pathStr.length()) {
            index = pathStr.indexOf(PATH_SEPARATOR_STR, index);
            if ((index == -1) || (index == (pathStr.length() - PATH_SEPARATOR_STR.length()))) {
                break;
            }
            index += PATH_SEPARATOR_STR.length();
            tmp[count] = index;
            count++;
        }
        return Arrays.copyOf(tmp, count);
    }

    @Override
    public RemoteCachingFileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return path.startsWith(PATH_SEPARATOR_STR);
    }

    @Override
    public RemoteCachingPath getRoot() {
        return getRoot(fileSystem);
    }

    @Override
    public Path getFileName() {
        if (getNameCount() == 0) {
            return null;
        }
        return getName(getNameCount() - 1);
    }

    @Override
    public Path getParent() {
        if (getNameCount() <= 1) {
            return null;
        }

        final RemoteCachingPath subpath = (RemoteCachingPath) subpath(0, getNameCount() - 1);
        if (isAbsolute()) {
            return new RemoteCachingPath(fileSystem, PATH_SEPARATOR_STR + subpath.toString());
        } else {
            return subpath;
        }
    }

    @Override
    public int getNameCount() {
        return nameOffsets.length;
    }

    @Override
    public Path getName(final int index) {
        return subpath(index, index + 1);
    }

    @Override
    public Path subpath(final int beginIndex, final int endIndex) {
        if ((beginIndex < 0) ||
                (endIndex > nameOffsets.length) ||
                (beginIndex >= endIndex)) {
            throw new IllegalArgumentException("Cannot get subpath [" +
                    beginIndex + ", " + endIndex + ") for path " + this);
        }

        // starting and ending nameOffsets
        final int start = nameOffsets[beginIndex];
        final int end = (endIndex == nameOffsets.length) ? path.length() : nameOffsets[endIndex];
        return new RemoteCachingPath(fileSystem, path.substring(start, end));
    }

    @Override
    public boolean startsWith(final Path other) {
        if (other.getNameCount() > getNameCount()) {
            return false;
        }
        if (other.isAbsolute() && !isAbsolute()) {
            return false;
        }

        for (int i = 0; i < other.getNameCount(); i++) {
            if (!getName(i).equals(other.getName(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean startsWith(final String other) {
        return startsWith(new RemoteCachingPath(fileSystem, other));
    }

    @Override
    public boolean endsWith(final Path other) {
        if (other.getNameCount() > getNameCount()) {
            return false;
        }
        if (other.isAbsolute() && (!isAbsolute() || (other.getNameCount() < getNameCount()))) {
            return false;
        }

        for (int i = 1; i <= other.getNameCount(); i++) {
            if (!getName(getNameCount() - i).equals(other.getName(other.getNameCount() - i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean endsWith(final String other) {
        return endsWith(new RemoteCachingPath(fileSystem, other));
    }

    @Override
    public RemoteCachingPath normalize() {
        if (normalizedPath.equals(path)) {
            return this;
        }
        return new RemoteCachingPath(fileSystem, normalizedPath);
    }

    @Override
    public Path resolve(final Path other) {
        final RemoteCachingPath otherPath = RemoteCachingFileSystemProvider.toRemoteCachePath(other);
        if (otherPath.isAbsolute() || path.isEmpty()) {
            return other;
        }
        if (otherPath.path.isEmpty()) {
            return this;
        }

        final String newPath;
        if (path.endsWith(PATH_SEPARATOR_STR)) {
            newPath = path + otherPath.path;
        } else {
            newPath = path + PATH_SEPARATOR_STR + otherPath.path;
        }
        return new RemoteCachingPath(fileSystem, newPath);
    }

    @Override
    public RemoteCachingPath resolve(final String other) {
        return (RemoteCachingPath) resolve(new RemoteCachingPath(fileSystem, other));
    }

    @Override
    public Path resolveSibling(final Path other) {
        final Path parent = getParent();
        return (parent == null) ? other : parent.resolve(other);
    }

    @Override
    public Path resolveSibling(final String other) {
        return resolveSibling(new RemoteCachingPath(fileSystem, other));
    }

    @Override
    public Path relativize(final Path other) {
        final RemoteCachingPath otherPath = RemoteCachingFileSystemProvider.toRemoteCachePath(other);
        if (!isAbsolute() || !otherPath.isAbsolute()) {
            throw new IllegalArgumentException("Cannot relativize relative paths");
        }

        RemoteCachingPath result = new RemoteCachingPath(fileSystem, "");
        int i = 0;
        for (; i < Math.min(getNameCount(), otherPath.getNameCount()); i++) {
            if (!getName(i).equals(otherPath.getName(i))) {
                break;
            }
        }

        if (i < getNameCount()) {
            throw new IllegalArgumentException("Relativizing path " + other + " by " + this + " not supported");
        }
        for (int j = i; j < otherPath.getNameCount(); j++) {
            result = (RemoteCachingPath) result.resolve(otherPath.getName(j));
        }

        return result;
    }

    @Override
    public URI toUri() {
        if (!isAbsolute()) {
            throw new IllegalStateException("Cannot convert relative path " + this + " to URI");
        }
        final StringBuilder pathBuilder = new StringBuilder(path.length()).append('/');
        for (int i = 0; i < getNameCount(); i++) {
            pathBuilder.append(getName(i)).append('/');
        }

        try {
            return new URI(fileSystem.provider().getScheme(), null, pathBuilder.toString(), null, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to construct URI from " + this, e);
        }
    }

    @Override
    public Path toAbsolutePath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path toRealPath(final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File toFile() {
        try {
            return fileSystem.getCachePath(this).toFile();
        } catch (final ExecutionException|IOException e) {
            throw new IllegalStateException("Unexpected error while getting cache path for " + this, e);
        }
    }

    @Override
    public WatchKey register(final WatchService watcher,
                             final WatchEvent.Kind<?>[] events,
                             final WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(final WatchService watcher, final WatchEvent.Kind<?>... events) throws
            IOException {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Iterator<Path> iterator() {
        return new Iterator<Path>() {
            private int i = 0;
            @Override
            public boolean hasNext() {
                return i < getNameCount();
            }

            @Override
            public Path next() {
                if (hasNext()) {
                    return getName(i++);
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public int compareTo(final Path other) {
        final RemoteCachingPath otherPath = RemoteCachingFileSystemProvider.toRemoteCachePath(other);
        return path.compareTo(otherPath.path);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RemoteCachingPath)) {
            return false;
        }
        final RemoteCachingPath paths = (RemoteCachingPath) o;
        return Objects.equal(path, paths.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(path);
    }

    private static String makeRelative(final String pathStr) {
        if (pathStr.startsWith(PATH_SEPARATOR_STR)) {
            return pathStr.substring(PATH_SEPARATOR_STR.length());
        } else {
            return pathStr;
        }
    }

    RemoteCachingPath asRelativePath() {
        if (isAbsolute()) {
            return new RemoteCachingPath(fileSystem, makeRelative(path));
        } else {
            return this;
        }
    }

    protected Object writeReplace() throws ObjectStreamException {
        return new PathProxy(this);
    }

    @Override
    public String toString() {
        return normalizedPath;
    }

    static RemoteCachingPath getRoot(final RemoteCachingFileSystem fs) {
        return new RemoteCachingPath(fs, PATH_SEPARATOR_STR);
    }

    /**
     * used to resolve a paths of different types
     */
    static <T extends Path> T resolve(final T basePath, final Path other) {
        T result = basePath;
        for (final Path component : other.normalize()) {
            result = (T) result.resolve(component.toString());
        }
        return result;
    }

    private static class PathProxy implements Serializable {
        private static final long serialVersionUID = -2658984992211825586L;

        private PathProxy(final Path path) {
            pathUri = path.toUri();
        }

        private final URI pathUri;

        protected Object readResolve() throws ObjectStreamException {
            return Paths.get(pathUri);
        }
    }
}
