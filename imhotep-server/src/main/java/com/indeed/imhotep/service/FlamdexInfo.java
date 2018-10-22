/*
 * Copyright (C) 2018 Indeed Inc.
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
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.nio.file.Files;
import java.nio.file.Path;

class FlamdexInfo {

    private static final Logger log = Logger.getLogger(FlamdexInfo.class);

    private final String shardName; // Shard directory name (i.e. including version) e.g. index20180722.19.20180722221746
    private final String shardId;   // Includes time range, but doesn't include versions. e.g. index20180722.19
    private final DateTime date;    // (Not the build timestamp but) the start time of the shard time range. e.g. 2018/07/22 19:00:00

    private final Object2LongArrayMap<String> fieldSizesInBytesCache =
            new Object2LongArrayMap<>(16);

    private static final String FIELD_PREFIX = "fld-";
    private static final String[] INT_FIELD_SUFFIXES = new String[]{".intdocs", ".intterms", ".intindex64/index.bin"};
    private static final String[] STRING_FIELD_SUFFIXES = new String[]{".strdocs", ".strterms", ".strindex/index.bin"};

    FlamdexInfo(final FlamdexReader reader) {
        fieldSizesInBytesCache.defaultReturnValue(-1);
        final Path shardDir = reader.getDirectory();
        if (shardDir != null) {
            this.shardId = (new ShardDir(shardDir)).getId();
            this.shardName = shardDir.getFileName().toString();
            this.date = dateOf();
        } else {
            this.shardId = null;
            this.shardName = null;
            this.date = null;
        }
    }

    String getShardName() {
        return shardName;
    }

    DateTime getDate() {
        return date;
    }

    /**
     * Return the size of a field, i.e. the sum of the sizes of its term and
     * doc files. Return zero if the field is unknown.
     */
    long getFieldSizeInBytes(final String fieldName, final boolean fieldIsString, final FlamdexReader reader) {
        final long cachedSize = fieldSizesInBytesCache.getLong(fieldName);
        if (cachedSize != -1) {
            return cachedSize;
        } else {
            final long size = calculateFieldSizeInBytes(fieldName, fieldIsString, reader);
            fieldSizesInBytesCache.put(fieldName, size);
            return size;
        }
    }

    private DateTime dateOf() {
        try {
            return ShardTimeUtils.parseStart(shardId);
        } catch (final Exception ex) {
            log.warn("cannot extract date from shard directory: '" + shardName + "'");
            return null;
        }
    }

    /**
     * Calculate the field size of fieldName in bytes
     *
     * @return size of the relevant field files in the shard's directory, or 0 if not found.
     */
    private long calculateFieldSizeInBytes(final String fieldName, final boolean fieldIsString, final FlamdexReader reader) {
        final Path dir = reader.getDirectory();
        if (dir == null) {
            return 0;
        }

        long size = 0;
        final String[] fileSuffixes = fieldIsString ? STRING_FIELD_SUFFIXES : INT_FIELD_SUFFIXES;
        for(String fileSuffix : fileSuffixes) {
            final String filePath = FIELD_PREFIX + fieldName + fileSuffix;
            try {
                size += Files.size(dir.resolve(filePath));
            } catch (Exception ignored) { } // only used for diagnostics so ignore if some files are missing
        }
        return size;
    }
}
