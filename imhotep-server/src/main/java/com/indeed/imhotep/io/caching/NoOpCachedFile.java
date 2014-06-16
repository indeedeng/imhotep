package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class NoOpCachedFile extends CachedFile {
    private File file;

    public NoOpCachedFile(RemoteFileCache cache, String path) {
        this.file = new File(path);
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    @Override
    public boolean isFile() {
        return file.isFile();
    }

    @Override
    public boolean isDirectory() {
        return file.isDirectory();
    }
    
    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public String[] list() {
        return file.list();
    }

    @Override
    public CachedFile[] listFiles() {
        final File[] files = file.listFiles();
        final CachedFile[] results = new CachedFile[files.length];
        
        for (int i = 0; i < results.length; i++) {
            File f = files[i];
            NoOpCachedFile result = new NoOpCachedFile(null, f.getAbsolutePath());
            result.file = f;
            results[i] = result;
        }
        return results;
    }

    @Override
    public File loadFile() throws IOException {
        return file;
    }

    @Override
    public boolean isCached() {
        return true;
    }

    @Override
    public File loadDirectory() throws IOException {
        return file;
    }

    @Override
    public String getCanonicalPath() throws IOException {
        return file.getCanonicalPath();
    }

    @Override
    public long length() {
        return file.length();
    }

}
