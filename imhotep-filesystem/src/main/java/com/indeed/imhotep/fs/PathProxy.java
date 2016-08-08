package com.indeed.imhotep.fs;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author darren
 */
class PathProxy implements Serializable {
    PathProxy(final Path path) {
        data = path.toUri();
    }

    private final URI data;

    Object readResolve() throws ObjectStreamException {
        return Paths.get(data);
    }
}
