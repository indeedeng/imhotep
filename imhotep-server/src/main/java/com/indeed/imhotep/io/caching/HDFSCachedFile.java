package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HDFSCachedFile extends CachedFile {

    public HDFSCachedFile(RemoteFileCache cache, String path) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean exists() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isFile() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDirectory() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String[] list() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public File loadFile() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isCached() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public CachedFile[] listFiles() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public File loadDirectory() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCanonicalPath() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long length() {
        // TODO Auto-generated method stub
        return 0;
    }

}
