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
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.indeed.imhotep.io.caching.RemoteFileSystem.RemoteFileInfo;

/**
 * Hello world!
 *
 */
public class CachedFile {
    public static final String DELIMITER = "/";
    public static final int CHAR_DELIMITER = '/';
    private static RemoteFileSystemMounter mounter = null;
    
    protected String fullPath;
    protected RemoteFileInfo info;
    protected RemoteFileSystem topFS;
    

    public static final synchronized CachedFile create(String path) {
        CachedFile ret;
        
        if (mounter == null) {
            try {
                mounter = new RemoteFileSystemMounter(null, "/", true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        ret = new CachedFile();
        ret.info = null;
        ret.fullPath = path;
        ret.topFS = mounter.getTopFileSystem();
        
        return ret;
    }
    
    public static final synchronized void initWithFile(String filename, 
                                                       String root) throws IOException {
        mounter = new RemoteFileSystemMounter(filename, root);
    }
    
    public static final synchronized void init(List<Map<String, Object>> configData, 
                                               String root, 
                                               boolean passthrough) throws IOException {
        mounter = new RemoteFileSystemMounter(configData, root, passthrough);
    }
    
    protected CachedFile() {
        this.topFS = null;
        this.info = null;
    }
    
    protected CachedFile(RemoteFileInfo info, RemoteFileSystem fs) {
        this.info = info;
        this.topFS = fs;
        this.fullPath = info.path;
    }

    
    public boolean exists() {
        if (info != null) {
            return true;
        }
        
        info = topFS.stat(fullPath);
        if (info == null) {
            return false;
        }
        
        return true;
    }
    
    
    public boolean isFile() {
        if (this.exists() && info.type == RemoteFileInfo.TYPE_FILE) {
            return true;
        }
        return false;
    }
    
    public boolean isDirectory() {
        if (this.exists() && info.type == RemoteFileInfo.TYPE_DIR) {
            return true;
        }
        return false;
    }
    
    public String getName() {
        return fullPath.substring(fullPath.lastIndexOf(CHAR_DELIMITER) + 1);
    }
    
    public String[] list() {
        final List<RemoteFileInfo> infos;
        final String[] result;
        
        infos = topFS.readDir(fullPath);
        if (infos == null) {
            return null;
        }
        
        result = new String[infos.size()];
        for (int i = 0; i< infos.size(); i++) {
            result[i] = infos.get(i).path;
        }
        
        return result;
    }
    
    public CachedFile[] listFiles() {
        final List<RemoteFileInfo> infos;
        final CachedFile[] result;
        
        infos = topFS.readDir(fullPath);
        if (infos == null) {
            return null;
        }
        
        result = new CachedFile[infos.size()];
        for (int i = 0; i< infos.size(); i++) {
            RemoteFileInfo info = infos.get(i);
            String path = buildPath(fullPath, info.path);
            info.path = path;
            result[i] = new CachedFile(info, topFS);
        }
        
        return result;
    }

    public File loadFile() throws IOException {
        return topFS.loadFile(fullPath);
    }
    
    public File loadDirectory() throws IOException {
        final Map<String,File> data;
        
        data = topFS.loadDirectory(fullPath, null);
        if (data == null) {
            return null;
        }
        
        return data.get(fullPath);
    }

    public String getCanonicalPath() throws IOException {
        return this.fullPath;
    }

    public long length() {
        File f;
        try {
            f = topFS.loadFile(fullPath);
        } catch (IOException e) {
            e.printStackTrace();
            return 0L;
        }
        return f.length();
    }

    public static String buildPath(String directory, String filename) {
        final String path = directory + DELIMITER + filename;
        return path.replace("//", "/");
    }


}
