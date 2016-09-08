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
import com.indeed.flamdex.simple.SimpleFlamdexFileFilter;
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

    private final String   shardId;
    private final DateTime date;
    private final long     sizeInBytes;

    private final Object2LongArrayMap<String> fieldSizesInBytes =
        new Object2LongArrayMap<String>(16);

    private static final String FIELD_PREFIX = "fld-";
    private static final String[] FIELD_EXTENSIONS = {
        "intdocs", "intterms", "strdocs", "strterms"
    };

    private static final Set<String> fieldExtensions =
        new ObjectArraySet<String>(FIELD_EXTENSIONS);

    FlamdexInfo(FlamdexReader reader) {
        this.shardId     = reader.getDirectory().getFileName().toString();
        this.date        = dateOf();
        this.sizeInBytes = initFieldSizes(reader);
    }

    String       getShardId() { return shardId;     }
    DateTime        getDate() { return date;        }
    long     getSizeInBytes() { return sizeInBytes; }

    /**
       Return the size of a field, i.e. the sum of the sizes of its term and
       doc files. Return zero if the field is unknown or from a Lucene index.
     */
    long getFieldSizeInBytes(final String fieldName) {
        return fieldSizesInBytes.getLong(fieldName);
    }

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
       Examine the field files in a shard, populating fieldSizesInBytes as we
       go. Because the FlamdexReader we're passed might be a wrapper, such as
       CachedFlamdexReaderReference, it might not be easy to determine whether
       we're dealing with an Indeed Flamdex or a Lucene index. The simplistic
       approach here is to first look for Flamdex files and in the face of no
       results just fall back to looking at all files in the directory.

       Since we don't have a good way of sizing fields in a Lucene index we'll
       end up with an empty fieldSizesInBytes map and the returned size will
       just be the sum of all file sizes.

       For Flamdex fields, we just sum the sizes of the doc and term files.

       @return sum of sizes of all field files in the shard's directory.
     */
    private long initFieldSizes(final FlamdexReader reader) {
        long result = 0;
        final Path dir = reader.getDirectory();
        final SimpleFlamdexFileFilter filter = new SimpleFlamdexFileFilter();

        boolean isFlamdex = false;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(dir, filter)) {
            // flamdex
            for (final Path child : children) {
                isFlamdex = true;
                final String fieldName = fieldNameOf(child);
                if (fieldName != null) {
                    long size = fieldSizesInBytes.getLong(fieldName);
                    size += Files.size(child);
                    fieldSizesInBytes.put(fieldName, size);
                    result += Files.size(child);
                }
            }
        } catch (final IOException e) {
            log.error("Error while getting flamdex field file sizes", e);
            return result;
        }

        // If the directory has no flamdex files in it,
        // then it is a lucene index
        if (!isFlamdex) {
            try (DirectoryStream<Path> children = Files.newDirectoryStream(dir)) {
                // lucene
                for (final Path child : children) {
                    result += Files.size(child);
                }
            } catch (final IOException e) {
                log.error("Error while getting lucene field file sizes", e);
                return result;
            }
        }

        return result;
    }

    /**
       Extract a field's name from one of its constituent files.

       @return The field's name or null if the file is not part of a field.
     */
    @Nullable
    private static final String fieldNameOf(final Path path) {
        final String name = path.getFileName().toString();
        if (name.startsWith(FIELD_PREFIX)) {
            final int begin = FIELD_PREFIX.length();
            final int end = name.lastIndexOf('.');
            if (end != -1 && fieldExtensions.contains(name.substring(end + 1))) {
                return name.substring(begin, end);
            }
        }
        return null;
    }
}
