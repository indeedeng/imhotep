package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public abstract class RemoteFileSystem {
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
        if (path.equals(DELIMITER)) {
            return "";
        }
        return path;
    }
    
    
    public abstract void copyFileInto(String fullPath, File localFile) throws IOException;
    public abstract File loadFile(String fullPath) throws IOException;
    public abstract RemoteFileInfo stat(String fullPath);
    public abstract List<RemoteFileInfo> readDir(String fullPath);
    
    public abstract String getMountPoint();


    public static class RemoteFileInfo {
        public static final int TYPE_FILE = 1;
        public static final int TYPE_DIR = 2;
        
        String path;
        int type;
        
        public RemoteFileInfo(String path, int type) {
            this.path = path;
            this.type = type;
        }
    }


    /*
     * Needed for Lucene indexes
     */
    public abstract Map<String, File> loadDirectory(String fullPath, File location) 
            throws IOException;


    public abstract InputStream getInputStreamForFile(String fullPath, 
                                                      long startOffset, 
                                                      long maxReadLength) throws IOException;

}
