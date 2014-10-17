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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ArrayListMultimap;

public class RemoteFileSystemMounter {
    public final String DELIMITER = "/";
    private String rootMountPoint;
    private ArrayListMultimap<String,RemoteFileSystem> pathToFS = ArrayListMultimap.create();
    private RemoteFileSystem topFS = null;

    
    private static final List<Map<String,Object>> loadConfigData(String filename) throws FileNotFoundException {
        final List<Map<String, Object>> configData;
        final Yaml yaml = new Yaml();
        
        if (filename == null ) {
            final InputStream in;

            in = ClassLoader.getSystemResourceAsStream("file-caching.yaml");
            if (in != null) {
                configData = (List<Map<String, Object>>)yaml.load(in);
            } else {
                return null;
            }
        } else {
            configData = (List<Map<String, Object>>)yaml.load(new FileInputStream(filename));
        }
        
        return configData;
    }
    
    public RemoteFileSystemMounter(String filename, String root) throws IOException {
        this(loadConfigData(filename), root, false);
    }
        
    public RemoteFileSystemMounter(List<Map<String, Object>> configData, 
                                   String root, boolean passthrough) throws IOException {
        final RemoteFileSystem rootFS;
        RemoteFileSystem mappingFS = null;
        
        rootMountPoint = root;
        if (! rootMountPoint.endsWith(DELIMITER)) {
            /* add delimiter to the end */
            rootMountPoint = rootMountPoint + DELIMITER;
        }
        
        /* create base no-op mount */
        rootFS = new NoOpRemoteFileSystem(null, null, this);
        addFileSystem("", rootFS);
        
        if (passthrough || configData == null) {
            this.topFS = rootFS;
            return;
        }
        
        /* create remapping fs */
        mappingFS = new RemappingRemoteFileSystem(new HashMap<String,String>(), rootFS, this);
        
        /* create top level cached fs */
        for (Map<String,Object> fsConfig : configData) {
            final String type = (String)fsConfig.get("type");
            if (type.equals("CACHED")) {
                this.topFS = new CachedRemoteFileSystem(fsConfig, mappingFS, this);
                break;
            }
        }
        if (this.topFS == null) {
            throw new RuntimeException("File system Cache configuration missing");
        }

        /* sort configured file systems by order */
        Collections.sort(configData, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                int p1;
                int p2;
                
                p1 = (Integer)o1.get("order");
                p2 = (Integer)o2.get("order");
                return p1 - p2;
            }
        });
        
        /* create file systems */
        RemoteFileSystem previous = rootFS;
        for (Map<String,Object> fsConfig : configData) {
            final RemoteFileSystem fs;
            final Object type;
            String relativeMP;
            
            type = fsConfig.get("type");
            if (type.equals("NOOP")) {
                fs = new NoOpRemoteFileSystem(fsConfig, previous, this);
            } else if (type.equals("S3")) {
                fs = new S3RemoteFileSystem(fsConfig, previous, this);
            } else if (type.equals("CACHED")) {
//                fs = new CachedRemoteFileSystem(fsConfig, previous, this);
                continue;
            } else if (type.equals("HDFS")) {
                fs = new HDFSRemoteFileSystem(fsConfig);
            } else if (type.equals("SQAR_AUTOMOUNTING")) {
                fs = new SqarAutomountingRemoteFileSystem(fsConfig, previous, this);
            } else {
                throw new RuntimeException("Unknown remote fs type: " + type);
            }
            
            relativeMP = (String)fsConfig.get("mountpoint");
            if (relativeMP.equals("/")) {
                relativeMP = "";
            } else {
                relativeMP = relativeMP + DELIMITER;
                relativeMP = relativeMP.replace("//", "/");
            }
            addFileSystem(relativeMP, fs);
            previous = fs;
        }
    }
    
    public synchronized RemoteFileSystem findMountPoint(String path) {
        final String[] parts;
        final String mountPointNoDelim;
        String remotePath;
        
        mountPointNoDelim = rootMountPoint.substring(0, rootMountPoint.length() - DELIMITER.length());
        if (path.equals(mountPointNoDelim) || path.equals(rootMountPoint)) {
            remotePath = "";
        } else if (path.startsWith(rootMountPoint)) {
            remotePath = path.substring(rootMountPoint.length());
            remotePath = RemoteFileSystem.cleanupPath(remotePath);
        } else {
            throw new RuntimeException("File \"" + path + "\" is not a remote file.");
        }
        
        parts = remotePath.split(DELIMITER);
        if (parts.length == 0) {
            /* get the default mapping */
            List<RemoteFileSystem> rootMappings = pathToFS.get("");
            return rootMappings.get(rootMappings.size() - 1);
        }
        
        String subPath = rootMountPoint;
        RemoteFileSystem fs = null;
        List<RemoteFileSystem> mappings;
        
        /* test for root mounts */
        mappings = pathToFS.get(subPath);
        if (! mappings.isEmpty()) {
            fs = mappings.get(mappings.size() - 1);
        }
        /* test for mounts further down */
        for (String part : parts) {
            subPath += part;
            subPath += DELIMITER;

            mappings = pathToFS.get(subPath);
            if (! mappings.isEmpty()) {
                fs = mappings.get(mappings.size() - 1);
            }
        }

        if (fs == null) {
            /* get the default mapping */
            List<RemoteFileSystem> rootMappings = pathToFS.get("");
            return rootMappings.get(rootMappings.size() - 1);
        }
        
        return fs;
    }
    
    public synchronized void addFileSystem(String path, RemoteFileSystem fs) {
        pathToFS.put(rootMountPoint + path, fs);
    }

    /*
     * Remove leading and trailing delimiters
     */
    protected String cleanupPath(String path) {
        if (path.startsWith(DELIMITER)) {
            /* remove delimiter from the beginning */
            path = path.substring(DELIMITER.length());
        }
        if (path.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            path = path.substring(0, path.length() - DELIMITER.length());
        }
        return path;
    }
    
    protected String getMountRelativePath(String fullpath, String mountPoint) {
        String relativePath;
        String mntPtNoDelim;
        
        if (mountPoint.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            mntPtNoDelim = mountPoint.substring(0, mountPoint.length() - DELIMITER.length());
        } else {
            mntPtNoDelim = mountPoint;
        }

        if (fullpath.endsWith(DELIMITER)) {
            /* remove delimiter from the end */
            relativePath = fullpath.substring(0, fullpath.length() - DELIMITER.length());
        } else {
            relativePath = fullpath;
        }
        
        if (! relativePath.startsWith(mntPtNoDelim)) {
            throw new RuntimeException("Path is not valid: " + fullpath);
        }
        relativePath = relativePath.substring(mntPtNoDelim.length());

        if (relativePath.startsWith(DELIMITER)) {
            /* remove delimiter from the beginning */
            relativePath = relativePath.substring(DELIMITER.length());
        }
        return relativePath;
    }

    public String buildPath(String ... parts) {
        if (parts.length == 0) return null;
        if (parts.length == 1) return parts[0];
        
        String path = parts[0];
        for (int i = 1; i < parts.length; i++) {
            path += DELIMITER + parts[i];
        }
        
        return path;
    }

    public String getRootMountPoint() {
        return this.rootMountPoint;
    }

    public RemoteFileSystem getTopFileSystem() {
        return this.topFS;
    }
}
