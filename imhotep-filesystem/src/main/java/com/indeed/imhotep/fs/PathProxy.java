package com.indeed.imhotep.fs;

import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by darren on 2/2/16.
 */
public class PathProxy implements Serializable {
    public PathProxy(Path path) {
        this.data = path.toUri();
    }

    public URI data;

    private Object readResolve() throws java.io.ObjectStreamException {
        Path result = Paths.get(data);
        return result;
    }
}
