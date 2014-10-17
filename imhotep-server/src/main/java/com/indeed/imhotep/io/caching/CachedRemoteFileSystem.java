/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.io.caching;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

public class CachedRemoteFileSystem extends RemoteFileSystem {
    private RemoteFileSystem parentFS;
    private String mountPoint;
    private RemoteFileSystemMounter mounter;
    private File localCacheDir;
    private LoadingCache<String, File> cache;

    public CachedRemoteFileSystem(Map<String,Object> settings, 
                                  RemoteFileSystem parentFS,
                                  RemoteFileSystemMounter mounter) throws IOException {
        final int cacheSize;
        final String cacheDir;
        
        this.parentFS = parentFS;
        mountPoint = (String)settings.get("mountpoint");
        if (! mountPoint.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            mountPoint = mountPoint + DELIMITER;
        }
        mountPoint = mounter.getRootMountPoint() + mountPoint;
        mountPoint = mountPoint.replace("//", "/");
        
        this.mounter = mounter;

        cacheDir = (String)settings.get("cache-dir");
        /* create directory if it does not already exist */
        localCacheDir = new File(cacheDir);
        localCacheDir.mkdir();
        
        cacheSize = (Integer)settings.get("cacheSizeMB");

        cache =
                CacheBuilder.newBuilder()
                            .initialCapacity(8192)
                            .maximumWeight(cacheSize * 1024)
                            .weigher(new Weigher<String, File>() {
                                public int weigh(String path, File cachedFile) {
                                    int kb;
                                    kb = (int)(cachedFile.length() / 1024);
                                    /* don't return weights of 0 */
                                    if (kb == 0) {
                                        kb = 1;
                                    }
                                    return kb;
                                }
                            })
                            .removalListener(new RemovalListener<String, File>() {
                                public void onRemoval(RemovalNotification<String, File> rn) {
                                    if (rn.getCause().equals(RemovalCause.REPLACED)) {
                                        /* don't delete replaced files */
                                        return;
                                    }
                                    removeFile(rn.getValue());
                                }
                            })
                            .build(new CacheLoader<String, File>() {
                                public File load(String path) throws Exception {
                                    return downloadFile(path);
                                }
                            });

        scanExistingFiles();
    }
    
    private void scanExistingFiles() throws IOException {
        final Iterator<File> filesInCache;
        final int prefixLen;
        
        prefixLen = localCacheDir.getCanonicalPath().length() + DELIMITER.length();
        filesInCache = FileUtils.iterateFiles(localCacheDir, 
                                              TrueFileFilter.INSTANCE, 
                                              TrueFileFilter.INSTANCE);
        while (filesInCache.hasNext()) {
            final File cachedFile = filesInCache.next();
            final String path = cachedFile.getCanonicalPath();
            String cachePath = path.substring(prefixLen);
            cache.put(cachePath, cachedFile);
        }
    }
    
    private void removeFile(File cachedFile) {
        cachedFile.delete();
    }
    
    private File downloadFile(String fullPath) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final File localFile;
        
        localFile = new File(localCacheDir, relativePath);
        /* create all the directories on the path to the file */
        localFile.getParentFile().mkdirs();

        parentFS.copyFileInto(fullPath, localFile);

        return localFile;
    }

    @Override
    public File loadFile(String fullPath) throws IOException {
        try {
            return cache.get(fullPath);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getMountPoint() {
        return this.mountPoint;
    }

    @Override
    public RemoteFileInfo stat(String fullPath) {
        return parentFS.stat(fullPath);
    }

    @Override
    public List<RemoteFileInfo> readDir(String fullPath) {
        return parentFS.readDir(fullPath);
    }

    @Override
    public void copyFileInto(String fullPath, File localFile) throws IOException {
        try {
            final File cachedFile;

            cachedFile = cache.get(fullPath);
            FileUtils.copyFile(cachedFile, localFile);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Map<String,File> loadDirectory(String fullPath, File location) throws IOException {
        final String relativePath = mounter.getMountRelativePath(fullPath, mountPoint);
        final Map<String,File> files;
        final File localDir;
        
        if (location != null) {
            throw new UnsupportedOperationException("CachedRemoteFileSystem does not "
                    + "support copying a directory.");
        }
        
        localDir = new File(localCacheDir, relativePath);
        /* create all the directories on the path to the file */
        localDir.getParentFile().mkdirs();
        
        files = parentFS.loadDirectory(fullPath, localDir);
        if (files == null) {
            return null;
        }
        
        for (Map.Entry<String, File> entry : files.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }
        files.put(fullPath, localDir);
        return files;
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
