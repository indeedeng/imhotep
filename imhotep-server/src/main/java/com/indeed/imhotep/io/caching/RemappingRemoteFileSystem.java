package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class RemappingRemoteFileSystem extends RemoteFileSystem {
    private RemoteFileSystemMounter mounter;
    private String mountPoint;

    public RemappingRemoteFileSystem(Map<String,String> settings, 
                                     RemoteFileSystem parent,
                                     RemoteFileSystemMounter mounter) {
        String mp;
        
        mp = settings.get("mountpoint");
        if (mp == null) {
            mp = "";
        } else {
            mp = mp.trim();
        }
        mountPoint = mp;
        mountPoint = mountPoint + DELIMITER;
        mountPoint = mounter.getRootMountPoint() + mountPoint;
        mountPoint = mountPoint.replace("//", "/");
        
        this.mounter = mounter;
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        fs.copyFileInto(fullPath, localFile);
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        return fs.loadFile(fullPath);
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        return fs.stat(fullPath);
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        return fs.readDir(fullPath);
    }

    @Override
    public String getMountPoint() {
        return this.mountPoint;
    }

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        return fs.loadDirectory(fullPath, location);
    }

    @Override
    public InputStream getInputStreamForFile(String fullPath, 
                                             long startOffset, 
                                             long maxReadLength) throws IOException {
        RemoteFileSystem fs;

        fs = mounter.findMountPoint(fullPath);
        return fs.getInputStreamForFile(fullPath, startOffset, maxReadLength);
    }

}
