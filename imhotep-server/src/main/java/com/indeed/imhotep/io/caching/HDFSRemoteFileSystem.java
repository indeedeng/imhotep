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
