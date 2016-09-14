package com.indeed.imhotep.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class NioPathUtil {

    private NioPathUtil() {
    }

    public static Path get(final String path) throws URISyntaxException {
        try {
            return Paths.get(new URI(path));
        } catch (final IllegalArgumentException e) {
            return Paths.get(path);
        }
    }
}
