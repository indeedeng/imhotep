package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SqarAutomountingRemoteFileSystem extends RemoteFileSystem {
    private static final String SUFFIX = ".sqar";
    
    private String mountPoint;
    private RemoteFileSystem parentFS;
    private RemoteFileSystemMounter mounter;

    public SqarAutomountingRemoteFileSystem(Map<String,String> settings, 
                              RemoteFileSystem parent,
                              RemoteFileSystemMounter mounter) throws IOException {
        
        this.parentFS = parent;
        this.mounter = mounter;
        
        mountPoint = settings.get("mountpoint").trim();
        if (! mountPoint.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            mountPoint = mountPoint + DELIMITER;
        }
        mountPoint = mounter.getRootMountPoint() + mountPoint;
        mountPoint = mountPoint.replace("//", "/");
    }
    
    private String scanPathForSqar(String fullPath) {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        String subPath = "";
        RemoteFileInfo info;
        
        /* test for mounts on the path */
        for (String part : relativePath.split(DELIMITER)) {
            String sqarName = part + SUFFIX + DELIMITER;

            info = parentFS.stat(this.mountPoint + subPath + sqarName);
            if (info != null) {
                /* found a sqar */
                return subPath + part + DELIMITER;
            }
            subPath += part + DELIMITER;
        }

        /* did not find anything */
        return null;
    }
    
    private SqarRemoteFileSystem mountNewSqarFS(String path) {
        final SqarRemoteFileSystem fs;
        final Map<String,String> settings;
        
        settings = new HashMap<String,String>();
        settings.put("mountpoint", path);
        fs = new SqarRemoteFileSystem(settings, parentFS, mounter);
        mounter.addFileSystem(path, fs);
        return fs;
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        final String sqarPath;
        final SqarRemoteFileSystem newFS;
        
        try {
            parentFS.copyFileInto(fullPath, localFile);
        } catch(IOException e) {
            sqarPath = scanPathForSqar(fullPath);
            if (sqarPath == null) {
                /* no sqar archives found */
                throw e;
            }
            newFS = mountNewSqarFS(sqarPath);
            
            /* now rerun query with new fs */
            newFS.copyFileInto(fullPath, localFile);
        }
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        final String sqarPath;
        final SqarRemoteFileSystem newFS;
       
        try {
            return parentFS.loadFile(fullPath);
        } catch(IOException e) {
            sqarPath = scanPathForSqar(fullPath);
            if (sqarPath == null) {
                /* no sqar archives found */
                throw e;
            }
            newFS = mountNewSqarFS(sqarPath);
            
            /* now rerun query with new fs */
            return newFS.loadFile(fullPath);
        }
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        final RemoteFileInfo current;
        final String sqarPath;
        final SqarRemoteFileSystem newFS;

        current = parentFS.stat(fullPath);
        if (current != null) {
            return current;
        }
        
        sqarPath = scanPathForSqar(fullPath);
        if (sqarPath == null) {
            /* no sqar archives found */
            return null;
        }
        newFS = mountNewSqarFS(sqarPath);
        
        /* now rerun query with new fs */
        return newFS.stat(fullPath);
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        List<RemoteFileInfo> infos;
        final String sqarPath;
        final SqarRemoteFileSystem newFS;
        
        infos = parentFS.readDir(fullPath);
        if (infos == null) {
            /* check the path for sqar archives */
            sqarPath = scanPathForSqar(fullPath);
            if (sqarPath == null) {
                /* no sqar archives found */
                return null;
            }
            newFS = mountNewSqarFS(sqarPath);
            
            /* now rerun query with new fs */
            infos = newFS.readDir(fullPath);
            if (infos == null) {
                /* still an error */
                return null;
            }
        }
        
        /* scan results for sqar archives */
        for (RemoteFileInfo info : infos)  {
            if (info.path.endsWith(SUFFIX)) {
                final String path = info.path;

                /* mount archive */
                mountNewSqarFS(path);  // Maybe archives should only be mounted with used?
                
                /* remove suffix */
                info.path = path.substring(0, path.length() - SUFFIX.length());
            }
        }
        
        return infos;
    }

    @Override
    public String getMountPoint() {
        return this.mountPoint;
    }

    @Override
    public Map<String, File> loadDirectory(String fullPath, File location) throws IOException {
        // not needed right now
        throw new UnsupportedOperationException();
    }
    
    @Override
    public InputStream getInputStreamForFile(String fullPath, 
                                             long startOffset, 
                                             long maxReadLength) throws IOException {
        return parentFS.getInputStreamForFile(fullPath, startOffset, maxReadLength);
    }

}
