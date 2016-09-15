package com.indeed.imhotep.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class NioPathUtil {

    private NioPathUtil() {
    }

    /**
     * Given a string representation of {@link Path} construct a corresponding path
     * This will first try to interpret the string as URI and get the path.
     * Otherwise, it will default to getting the path for the default fs.
     * @param path path in string, URI or non-URI
     * @return the constructed path
     * @throws URISyntaxException
     */
    public static Path get(final String path) throws URISyntaxException {
        try {
            return Paths.get(new URI(path));
        } catch (final IllegalArgumentException e) {
            return Paths.get(path);
        }
    }
}
