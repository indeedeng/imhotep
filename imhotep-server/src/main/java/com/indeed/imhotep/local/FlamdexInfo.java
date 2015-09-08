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

package com.indeed.imhotep.local;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexFileFilter;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.client.ShardTimeUtils;

import java.io.File;
import java.io.FileFilter;

import org.apache.log4j.Logger;

import org.joda.time.DateTime;
import org.joda.time.Interval;

class FlamdexInfo {

    private static final Logger log = Logger.getLogger(FlamdexInfo.class);

    static Abstract get(FlamdexReader reader) {
        if (reader instanceof SimpleFlamdexReader)
            return new FlamdexInfo.Simple((SimpleFlamdexReader) reader);
        if (reader instanceof LuceneFlamdexReader)
            return new FlamdexInfo.Lucene((LuceneFlamdexReader) reader);
        throw new IllegalArgumentException("unsupported class: " +
                                           reader.getClass().getSimpleName());
    }

    static abstract class Abstract {

        private final DateTime date;
        private final long     sizeInBytes;

        protected Abstract(FlamdexReader reader) {
            this.date        = dateOf(reader);
            this.sizeInBytes = sizeInBytesOf(reader);
        }

        DateTime     getDate() { return date;        }
        long     sizeInBytes() { return sizeInBytes; }

        protected DateTime dateOf(FlamdexReader reader) {
            final String shardId = reader.getDirectory();
            try {
                final Interval interval = ShardTimeUtils.parseInterval(shardId);
                final DateTime start    = interval.getStart();
                return new DateTime(start.year().get(),
                                    start.monthOfYear().get(),
                                    start.dayOfMonth().get(),
                                    0, 0);
            }
            catch (Exception ex) {
                log.warn("cannot extract date from shard directory", ex);
                return null;
            }
        }

        protected long sizeInBytesOf(FlamdexReader reader) {
            long result = 0;
            final File dir = new File(reader.getDirectory());
            final File[] children = dir.listFiles(getFileFilter());
            if (children != null) {
                for (File child: children) {
                    result += child.length();
                }
            }
            return result;
        }

        protected abstract FileFilter getFileFilter();
    }

    static class Simple extends Abstract {

        Simple(SimpleFlamdexReader reader) { super(reader); }

        @Override
        protected FileFilter getFileFilter() {
            return new SimpleFlamdexFileFilter();
        }
    }

    static class Lucene extends Abstract {

        Lucene(LuceneFlamdexReader reader) { super(reader); }

        @Override
        protected FileFilter getFileFilter() {
            return new FileFilter() {
                public boolean accept(File dir) { return true; }
            };
        }
    }
}
