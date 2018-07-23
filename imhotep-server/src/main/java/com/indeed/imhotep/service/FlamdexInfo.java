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
import com.indeed.flamdex.simple.SimpleFlamdexFileFilter;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.ShardTimeUtils;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

class FlamdexInfo {

    private static final Logger log = Logger.getLogger(FlamdexInfo.class);

    private final String indexName; // Shard directory name (i.e. including version) e.g. index20180722.19.20180722221746
    private final String shardId;   // Includes time range, but doesn't include versions. e.g. index20180722.19
    private final DateTime date;    // (Not the build timestamp but) the start time of the shard time range. e.g. 2018/07/22 19:00:00

    private final Object2LongArrayMap<String> fieldSizesInBytesCache =
            new Object2LongArrayMap<>(16);

    private static final String FIELD_PREFIX = "fld-";
    private static final Set<String> FIELD_EXTENSIONS = new ObjectArraySet<>(new String[]{"intdocs", "intterms", "strdocs", "strterms"});

    FlamdexInfo(final FlamdexReader reader) {
        fieldSizesInBytesCache.defaultReturnValue(-1);
        final Path shardDir = reader.getDirectory();
        if (shardDir != null) {
            this.shardId = (new ShardDir(shardDir)).getId();
            this.indexName = shardDir.getFileName().toString();
            this.date = dateOf();
        } else {
            this.shardId = null;
            this.indexName = null;
            this.date = null;
        }
    }

    String getIndexName() {
        return indexName;
    }

    DateTime getDate() {
        return date;
    }

    /**
     * Return the size of a field, i.e. the sum of the sizes of its term and
     * doc files. Return zero if the field is unknown or from a Lucene index.
     */
    long getFieldSizeInBytes(final String fieldName, final FlamdexReader reader) {
        final long cachedSize = fieldSizesInBytesCache.getLong(fieldName);
        if (cachedSize != -1) {
            return cachedSize;
        } else {
            final long size = calculateFieldSizeInBytes(fieldName, reader);
            fieldSizesInBytesCache.put(fieldName, size);
            return size;
        }
    }

    private DateTime dateOf() {
        try {
            return ShardTimeUtils.parseStart(shardId);
        } catch (final Exception ex) {
            log.warn("cannot extract date from shard directory: '" + indexName + "'");
            return null;
        }
    }

    /**
     * Calculate the field size of fieldName in bytes. Because the FlamdexReader
     * we're passed might be a wrapper, such as CachedFlamdexReaderReference,
     * it might not be easy to determine whether we're dealing with an Indeed
     * Flamdex or a Lucene index. The simplistic approach here is to first look
     * for Flamdex files and in the face of no results simply return 0.
     * <p>
     * Since we don't have a good way of sizing fields in a Lucene index we'll
     * simply return 0 for the field size in the Lucene case.
     *
     * @return size of the relevant field files in the shard's directory, or 0 if not found.
     */
    private long calculateFieldSizeInBytes(final String fieldName, final FlamdexReader reader) {
        final Path dir = reader.getDirectory();
        if (dir == null) {
            return 0;
        }

        long size = 0;
        final SimpleFlamdexFileFilter filter = new SimpleFlamdexFileFilter();
        try (final DirectoryStream<Path> children = Files.newDirectoryStream(dir, filter)) {
            for (final Path child: children) {
                final String childFieldName = fieldNameOf(child);
                if ((childFieldName != null) && childFieldName.equals(fieldName)) {
                    size += Files.size(child);
                }
            }
        } catch (final IOException e) {
            log.error("Error while getting flamdex field file sizes", e);
        }
        return size;
    }

    /**
     * Extract a field's name from one of its constituent files.
     *
     * @return The field's name or null if the file is not part of a field.
     */
    @Nullable
    private static String fieldNameOf(final Path path) {
        final String name = path.getFileName().toString();
        if (name.startsWith(FIELD_PREFIX)) {
            final int begin = FIELD_PREFIX.length();
            final int end = name.lastIndexOf('.');
            if ((end != -1) && FIELD_EXTENSIONS.contains(name.substring(end + 1))) {
                return name.substring(begin, end);
            }
        }
        return null;
    }
}
