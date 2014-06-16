package com.indeed.imhotep.io.caching;

import java.io.File;
import java.util.concurrent.ExecutionException;

public abstract class RemoteFileCache {
    public static final String DELIMITER = "/";

    /*
     * Remove leading and trailing delimiters
     */
    protected static String cleanupPath(String path) {
        if (path.startsWith(DELIMITER)) {
            /* remove delimiter from the beginning */
            path = path.substring(DELIMITER.length());
        }
        if (path.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            path = path.substring(0, path.length() - DELIMITER.length());
        }
        return path;
    }
    
    
    public abstract File getFile(String path) throws ExecutionException;
    public abstract boolean contains(String path);
}
