package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class HDFSRemoteFileSystem extends RemoteFileSystem {

    public HDFSRemoteFileSystem(Map<String,Object> settings) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getMountPoint() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getInputStreamForFile(String fullPath, long startOffset, long maxReadLength) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }


}
