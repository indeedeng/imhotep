/*
 * Copyright (C) 2015 Indeed Inc.
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
package com.indeed.imhotep.service;

import com.indeed.imhotep.io.Shard;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ShardDir {
    private static final Pattern VERSION_PATTERN = Pattern.compile("^(.+)\\.(\\d{14})$");

    private final String name;
    private final String indexDir;
    private final String id;
    private final long   version;

    ShardDir(File file) throws IOException {

        this.name     = file.getName();
        this.indexDir = file.getCanonicalPath();

        final Matcher matcher = VERSION_PATTERN.matcher(name);
        if (matcher.matches()) {
            this.id      = matcher.group(1);
            this.version = Long.parseLong(matcher.group(2));
        }
        else {
            this.id      = name;
            this.version = 0L;
        }
    }

    ShardDir(String path) throws IOException { this(new File(path)); }

    String       getId() { return id;       }
    long    getVersion() { return version;  }
    String     getName() { return name;     }
    String getIndexDir() { return indexDir; }

    boolean isNewerThan(Shard shard) {
        if (shard == null) return true;
        if (getVersion() > shard.getShardVersion()) return true;
        final File thisFile = new File(getIndexDir());
        final File thatFile = new File(shard.getIndexDir());
        return thisFile.lastModified() > thatFile.lastModified();
    }
}
