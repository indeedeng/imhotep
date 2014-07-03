package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

public class NoOpRemoteFileSystem extends RemoteFileSystem {

    public NoOpRemoteFileSystem(Map<String,Object> settings, 
                                RemoteFileSystem parent,
                                RemoteFileSystemMounter mounter) {
        
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final File srcFile = new File(fullPath);
        
        FileUtils.copyFile(srcFile, localFile);
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        return new File(fullPath);
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        final File file = new File(fullPath);
        final int type;
        
        if (file.isFile()) {
            type = RemoteFileInfo.TYPE_FILE;
        } else if (file.isDirectory()) {
            type = RemoteFileInfo.TYPE_DIR;
        } else {
            /* file probably does not exist */
            return null;
        }
        
        return new RemoteFileInfo(fullPath, type);
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        final File dir = new File(fullPath);
        final List<RemoteFileInfo> results = new ArrayList<RemoteFileInfo>(100);
        
        if (! dir.isDirectory()) {
            return null;
        }
        
        for (File f : dir.listFiles()) {
            final int type;
            final RemoteFileInfo info;
            
            if (f.isFile()) {
                type = RemoteFileInfo.TYPE_FILE;
            } else {
                type = RemoteFileInfo.TYPE_DIR;
            }
            info = new RemoteFileInfo(f.getName(), type);
            results.add(info);
        }
        
        return results;
    }

    @Override
    public String getMountPoint() {
        return "";
    }

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        final Map<String, File> results = new HashMap<String,File>(100);
        final File dir = new File(fullPath);
        
        if (location != null) {
            throw new UnsupportedOperationException("No-op does not support moving files.");
        }
        
        if (! dir.isDirectory()) {
            throw new IOException(fullPath + " is not a directory.");
        }
        
        Iterator<File> iter = FileUtils.iterateFiles(dir, null, true);
        while (iter.hasNext()) {
            final File f = iter.next();
            results.put(f.getAbsolutePath(), f);
        }
        return results;
    }

    @Override
    public InputStream getInputStreamForFile(String fullPath, 
                                             long startOffset, 
                                             long maxReadLength) throws IOException {
        final File file;
        final FileInputStream fis;
        
        file = loadFile(fullPath);
        fis = new FileInputStream(file);
        fis.skip(startOffset);
        return fis;
    }

}
