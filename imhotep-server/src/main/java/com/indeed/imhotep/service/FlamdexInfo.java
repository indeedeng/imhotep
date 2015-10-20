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

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexFileFilter;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.client.ShardTimeUtils;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import org.joda.time.DateTime;

class FlamdexInfo {

    private static final Logger log = Logger.getLogger(FlamdexInfo.class);

    private final String   shardId;
    private final DateTime date;
    private final long     sizeInBytes;

    FlamdexInfo(FlamdexReader reader) {
        this.shardId     = FilenameUtils.getName(reader.getDirectory());
        this.date        = dateOf();
        this.sizeInBytes = sizeInBytesOf(reader);
    }

    String       getShardId() { return shardId;     }
    DateTime        getDate() { return date;        }
    long     getSizeInBytes() { return sizeInBytes; }

    private DateTime dateOf() {
        try {
            return ShardTimeUtils.parseStart(shardId);
        }
        catch (Exception ex) {
            log.warn("cannot extract date from shard directory: '" + shardId + "'");
            return null;
        }
    }

    /**
       Add up all the relevant files in a shard's directory to compute its
       size. Because the FlamdexReader we're passed might be a wrapper, such as
       CachedFlamdexReaderReference, it might not be easy to determine whether
       we're dealing with an Indeed Flamdex or a Lucene index. The simplistic
       approach here is to first look for Flamdex files and in the face of no
       results just fall back to looking at all files in the directory.

       @return sum of sizes of all relevant files in the shard's directory.
     */
    private long sizeInBytesOf(FlamdexReader reader) {
        long result = 0;
        final File dir = new File(reader.getDirectory());
        File[] children = dir.listFiles(new SimpleFlamdexFileFilter());
        children = children.length > 0 ? children : dir.listFiles();
        if (children != null) {
            for (File child: children) {
                result += child.length();
            }
        }
        else {
            log.warn("cannot list files in " + reader.getDirectory());
        }
        return result;
    }
}
