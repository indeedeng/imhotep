package com.indeed.imhotep.fs;

import com.google.common.base.Objects;
import com.indeed.imhotep.client.Host;
import org.apache.commons.collections.comparators.NullComparator;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xweng
 *
 * A remote path to represent the file location in other daemons
 * absolute path format: /remote/host:port/path/to/file
 * relative path format: /path/to/file
 */
public class P2PCachingPath extends RemoteCachingPath {

    private static final String PATH_PREFIX = "remote";

    // TODO: ^/(?!ignoreme|ignoreme2|ignoremeN)([a-z0-9]+)$
    // format: /remote/host:port/
    private static final String P2P_PATH_REGEX = new StringBuilder()
            .append("^")
            .append(PATH_SEPARATOR_STR)
            .append(PATH_PREFIX)
            .append(PATH_SEPARATOR_STR)
            .append("[^\\:/]+:[0-9]+")
            .append(PATH_SEPARATOR_STR)
            .toString();
    private static final Pattern P2P_PATH_PATTERN = Pattern.compile(P2P_PATH_REGEX);

    private static final Comparator<Host> NULL_SAFE_COMPARATOR = new NullComparator(false);

    private final RemoteCachingFileSystem fileSystem;

    // the host information, null if it's a relative path
    @Nullable
    private final Host peerHost;

    P2PCachingPath(final RemoteCachingFileSystem fileSystem, final String path, @Nullable final Host peerHost) {
        super(fileSystem, path);
        this.fileSystem = fileSystem;
        this.peerHost = peerHost;
    }

    P2PCachingPath(final RemoteCachingFileSystem fileSystem, final String path) {
        this(fileSystem, path, null);
    }

    static P2PCachingPath newP2PCachingPath(final RemoteCachingFileSystem fileSystem, final String path) {
        final Matcher matcher = P2P_PATH_PATTERN.matcher(path);

        // relative path
        if (!matcher.lookingAt()) {
            if (path.startsWith(PATH_SEPARATOR_STR)) {
                throw new IllegalArgumentException("Not a valid relative path");
            }
            return new P2PCachingPath(fileSystem, path);
        }

        final String[] prefixItems = matcher.group(0).split(PATH_SEPARATOR_STR);
        final Host host = Host.valueOf(prefixItems[prefixItems.length-1]);
        // it always be an absolute path if it has host information
        return new P2PCachingPath(fileSystem, path.replaceAll(P2P_PATH_REGEX, PATH_SEPARATOR_STR), host);
    }

    static boolean isAbsoluteP2PCachingPath(final String path) {
        return P2P_PATH_PATTERN.matcher(path).lookingAt();
    }

    public static P2PCachingPath toP2PCachingPath(final RemoteCachingPath rootPath, final Path realPath, final Host host) {
        if (realPath instanceof P2PCachingPath) {
            throw new IllegalArgumentException("realPath is already a p2p caching real");
        }
        if (!realPath.isAbsolute()) {
            throw new IllegalArgumentException("Can't convert a relative path to p2pCachingPath");
        }

        return new P2PCachingPath(rootPath.getFileSystem(), realPath.toString(), host);
    }

    static P2PCachingPath toP2PCachingPath(final RemoteCachingPath rootPath, final String path, final Host host) {
        return toP2PCachingPath(rootPath, Paths.get(URI.create(path)), host);
    }

    @Nullable
    Host getPeerHost() {
        return peerHost;
    }

    public RemoteCachingPath getRealPath() {
        return new RemoteCachingPath(fileSystem, super.toString());
    }

    @Override
    public P2PCachingPath getRoot() {
        if (!isAbsolute()) {
            throw new IllegalArgumentException("Couldn't get root path for a relative path");
        }
        return new P2PCachingPath(fileSystem, PATH_SEPARATOR_STR, peerHost);
    }

    @Override
    public Path getParent() {
        final Path parentPath = super.getParent();
        return parentPath != null ? new P2PCachingPath(fileSystem, parentPath.toString(), peerHost) : null;
    }

    @Override
    public Path subpath(final int beginIndex, final int endIndex) {
        final Path subPath = super.subpath(beginIndex, endIndex);
        return new P2PCachingPath(fileSystem, subPath.toString(), peerHost);
    }

    @Override
    public RemoteCachingPath normalize() {
        final Path normalizedPath = super.normalize();
        if (equals(normalizedPath)) {
            return this;
        }
        return new P2PCachingPath(fileSystem, normalizedPath.toString(), peerHost);
    }

    @Override
    public Path resolve(final Path other) {
        final Path resolvedPath = super.resolve(other);
        return new P2PCachingPath(fileSystem, resolvedPath.toString(), peerHost);
    }


    @Override
    public Path relativize(final Path other) {
        final Path relativizedPath = super.relativize(other);
        return new P2PCachingPath(fileSystem, relativizedPath.toString(), peerHost);
    }

    public URI toUri() {
        final URI baseUri = super.toUri();
        try {
            return new URI(
                    fileSystem.provider().getScheme(),
                    null,
                    appendHostIfNecessary(baseUri.getRawPath(), peerHost),
                    null,
                    null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to construct URI from " + this, e);
        }
    }

    @Override
    public int compareTo(final Path other) {
        final P2PCachingPath otherPath = RemoteCachingFileSystemProvider.toP2PCachingPath(other);
        final int result = NULL_SAFE_COMPARATOR.compare(peerHost, otherPath.peerHost);
        if (result != 0) {
            return result;
        }
        return super.compareTo(otherPath);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final P2PCachingPath that = (P2PCachingPath) o;
        final int result = NULL_SAFE_COMPARATOR.compare(peerHost, that.peerHost);
        if (result != 0) {
            return false;
        }
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), peerHost);
    }

    RemoteCachingPath asRelativePath() {
        final Path relativePath = super.asRelativePath();
        if (equals(relativePath)) {
            return this;
        }
        return new P2PCachingPath(fileSystem, relativePath.toString(), peerHost);
    }

    private String appendHostIfNecessary(final String path, final Host host) {
        if (isAbsolute()) {
            return new StringBuilder()
                    .append(PATH_SEPARATOR_STR)
                    .append(PATH_PREFIX)
                    .append(PATH_SEPARATOR_STR)
                    .append(host.getHostname())
                    .append(":")
                    .append(host.getPort())
                    .append(path)
                    .toString();
        }
        return path;
    }

    @Override
    public String toString() {
        return appendHostIfNecessary(super.toString(), peerHost);
    }
}