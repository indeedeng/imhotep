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
