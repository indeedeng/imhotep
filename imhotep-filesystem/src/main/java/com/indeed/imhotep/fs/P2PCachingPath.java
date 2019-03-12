package com.indeed.imhotep.fs;

import com.indeed.imhotep.client.Host;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * @author xweng
 */
public class P2PCachingPath extends RemoteCachingPath {
    private static final String PATH_PREFIX = "remote";
    // TODO: make the regex stricter
    private static final Pattern P2P_PATH_PATTERN = Pattern.compile("^/" + PATH_PREFIX + "/[^\\:]+:[0-9]+/.*");

    private final Host peerHost;
    private final RemoteCachingPath realPath;

    P2PCachingPath(final RemoteCachingFileSystem fs, final String path) {
        super(fs, path);

        if (!isP2PCachingPath(path)) {
            throw new IllegalArgumentException("Not a valid p2p caching path: " + path);
        }

        final String hostStr = getName(1).toString();
        peerHost = Host.valueOf(hostStr);
        final Path subPath = subpath(2, getNameCount());
        realPath = getRoot().resolve(PATH_SEPARATOR_STR + subPath.toString());
    }

    static boolean isP2PCachingPath(final String path) {
        return P2P_PATH_PATTERN.matcher(path).matches();
    }

    public static P2PCachingPath toP2PCachingPath(final RemoteCachingPath rootPath, final RemoteCachingPath path, final Host host) {
        if (path instanceof P2PCachingPath) {
            throw new IllegalArgumentException("path is already a p2p caching path");
        }

        // convert absolute path to relative path
        String pathStr = path.toString();
        if (!PATH_SEPARATOR_STR.equals(pathStr) && pathStr.startsWith(PATH_SEPARATOR_STR)) {
            pathStr = pathStr.substring(PATH_SEPARATOR_STR.length());
        }
        final Path fakeRemotePath = rootPath.resolve(PATH_PREFIX).resolve(host.toString()).resolve(pathStr);
        return (P2PCachingPath) Paths.get(fakeRemotePath.toUri());
    }

    static P2PCachingPath toP2PCachingPath(final RemoteCachingPath rootPath, final String path, final Host host) {
        return toP2PCachingPath(rootPath, (RemoteCachingPath) Paths.get(URI.create(path)), host);
    }

    public Host getPeerHost() {
        return peerHost;
    }

    public RemoteCachingPath getRealPath() {
        return realPath;
    }
}
