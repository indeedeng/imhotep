package com.indeed.imhotep.fs;

import com.indeed.imhotep.client.Host;

import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * @author xweng
 */
public class P2PCachingPath extends RemoteCachingPath {
    // TODO: make the regex stricter
    private static final Pattern P2P_PATH_PATTERN = Pattern.compile("^/remote/[^\\:]+:[0-9]+/.*");

    private final Host peerHost;
    private final RemoteCachingPath realPath;

    static boolean isP2PCachingPath(final String path) {
        return P2P_PATH_PATTERN.matcher(path).matches();
    }

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

    Host getPeerHost() {
        return peerHost;
    }

    RemoteCachingPath getRealPath() {
        return realPath;
    }
}
