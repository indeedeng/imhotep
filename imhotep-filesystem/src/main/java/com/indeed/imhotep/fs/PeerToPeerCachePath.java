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
 * A remote path to represent files location in other daemons
 * absolute path format: /remote/host:port/path/to/file
 * relative path format: /path/to/file
 */
public class PeerToPeerCachePath extends RemoteCachingPath {

    private static final String PATH_PREFIX = "remote";

    // format: /remote/host:port/
    private static final String PEER_TO_PEER_CACHE_PATH_REGEX =
            "^" + PATH_SEPARATOR_STR + PATH_PREFIX + PATH_SEPARATOR_STR + "[^\\\\:?" + PATH_SEPARATOR_STR + "]+:[0-9]+" + PATH_SEPARATOR_STR;
    private static final Pattern PEER_TO_PEER_CACHE_PATH_PATTERN = Pattern.compile(PEER_TO_PEER_CACHE_PATH_REGEX);

    private static final Comparator<Host> NULL_SAFE_COMPARATOR = new NullComparator(false);

    private final RemoteCachingFileSystem fileSystem;

    // the host information, null if it's a relative path
    @Nullable
    private final Host remoteHost;

    private PeerToPeerCachePath(final RemoteCachingFileSystem fileSystem, final String path, @Nullable final Host remoteHost) {
        super(fileSystem, path);
        this.fileSystem = fileSystem;
        this.remoteHost = remoteHost;
    }

    private PeerToPeerCachePath(final RemoteCachingFileSystem fileSystem, final String path) {
        this(fileSystem, path, null);
    }

    static PeerToPeerCachePath newPeerToPeerCachePath(final RemoteCachingFileSystem fileSystem, final String path) {
        final Matcher matcher = PEER_TO_PEER_CACHE_PATH_PATTERN.matcher(path);

        // resolve as an absolute path
        if (!matcher.lookingAt()) {
            if (path.startsWith(PATH_SEPARATOR_STR)) {
                throw new IllegalArgumentException("The given path is neither a valid peer to peer cache absolute path, nor a relative path:" + path);
            }
            return new PeerToPeerCachePath(fileSystem, path);
        }

        // resolve as an absolute path
        final String[] prefixItems = matcher.group(0).split(PATH_SEPARATOR_STR);
        final Host host = Host.valueOf(prefixItems[prefixItems.length-1]);
        return new PeerToPeerCachePath(fileSystem, path.replaceAll(PEER_TO_PEER_CACHE_PATH_REGEX, PATH_SEPARATOR_STR), host);
    }

    static boolean isAbsolutePeerToPeerCachePath(final String path) {
        return PEER_TO_PEER_CACHE_PATH_PATTERN.matcher(path).lookingAt();
    }

    /** convert a normal path to PeerToPeerCachePath */
    public static PeerToPeerCachePath toPeerToPeerCachePath(final RemoteCachingPath rootPath, final Path realPath, final Host host) {
        if (realPath instanceof PeerToPeerCachePath) {
            throw new IllegalArgumentException("realPath is already a peer to peer cache path");
        }
        if (!realPath.isAbsolute()) {
            throw new IllegalArgumentException("Can't convert a relative path to peer to peer cache path");
        }
        return new PeerToPeerCachePath(rootPath.getFileSystem(), realPath.toString(), host);
    }

    static PeerToPeerCachePath toPeerToPeerCachePath(final RemoteCachingPath rootPath, final String path, final Host host) {
        return toPeerToPeerCachePath(rootPath, Paths.get(URI.create(path)), host);
    }

    @Nullable
    Host getRemoteHost() {
        return remoteHost;
    }

    public RemoteCachingPath getRealPath() {
        return new RemoteCachingPath(fileSystem, super.toString());
    }

    @Override
    public PeerToPeerCachePath getRoot() {
        if (!isAbsolute()) {
            throw new IllegalArgumentException("Couldn't get root path for a relative path");
        }
        return new PeerToPeerCachePath(fileSystem, PATH_SEPARATOR_STR, remoteHost);
    }

    @Override
    public Path getParent() {
        final Path parentPath = super.getParent();
        return parentPath != null ? new PeerToPeerCachePath(fileSystem, parentPath.toString(), remoteHost) : null;
    }

    @Override
    public Path subpath(final int beginIndex, final int endIndex) {
        final Path subPath = super.subpath(beginIndex, endIndex);
        return new PeerToPeerCachePath(fileSystem, subPath.toString(), remoteHost);
    }

    @Override
    public RemoteCachingPath normalize() {
        final Path normalizedPath = super.normalize();
        if (equals(normalizedPath)) {
            return this;
        }
        return new PeerToPeerCachePath(fileSystem, normalizedPath.toString(), remoteHost);
    }

    @Override
    public Path resolve(final Path other) {
        final Path resolvedPath = super.resolve(other);
        return new PeerToPeerCachePath(fileSystem, resolvedPath.toString(), remoteHost);
    }


    @Override
    public Path relativize(final Path other) {
        final Path relativizedPath = super.relativize(other);
        return new PeerToPeerCachePath(fileSystem, relativizedPath.toString(), remoteHost);
    }

    public URI toUri() {
        final URI baseUri = super.toUri();
        try {
            return new URI(
                    fileSystem.provider().getScheme(),
                    null,
                    appendHostIfNecessary(baseUri.getRawPath(), remoteHost),
                    null,
                    null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to construct URI from " + this, e);
        }
    }

    @Override
    public int compareTo(final Path other) {
        final PeerToPeerCachePath otherPath = RemoteCachingFileSystemProvider.toPeerToPeerCachePath(other);
        final int result = NULL_SAFE_COMPARATOR.compare(remoteHost, otherPath.remoteHost);
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
        final PeerToPeerCachePath that = (PeerToPeerCachePath) o;
        final int result = NULL_SAFE_COMPARATOR.compare(remoteHost, that.remoteHost);
        if (result != 0) {
            return false;
        }
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), remoteHost);
    }

    RemoteCachingPath asRelativePath() {
        final Path relativePath = super.asRelativePath();
        if (equals(relativePath)) {
            return this;
        }
        return new PeerToPeerCachePath(fileSystem, relativePath.toString(), remoteHost);
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
        return appendHostIfNecessary(super.toString(), remoteHost);
    }
}