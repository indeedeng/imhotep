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
 package com.indeed.flamdex;

import com.google.common.base.Throwables;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.fieldcache.FieldCacherUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jsgroth
 *
 * FlamdexReader base class that provides implementations of some of the {@link FlamdexReader} methods
 *
 * at the moment of this comment's writing, {@link FlamdexReader#getMetric} and {@link FlamdexReader#memoryRequired} are implemented here
 */
public abstract class AbstractFlamdexReader implements FlamdexReader {
    protected final Path directory;
    protected final int numDocs;
    protected final boolean useMMapMetrics;

    public static final class MinMax {
        public long min;
        public long max;
    }

    protected final ConcurrentMap<String, MinMax> metricMinMaxes;

    protected AbstractFlamdexReader(final Path directory, final int numDocs, final boolean useMMapMetrics) {
        this.directory = directory;
        this.numDocs = numDocs;
        this.useMMapMetrics = useMMapMetrics && directory != null;

        this.metricMinMaxes = new ConcurrentHashMap<>();
    }

    @Override
    public StringValueLookup getStringLookup(final String field) throws FlamdexOutOfMemoryException {
        try {
            return FieldCacherUtil.newStringValueLookup(field, this);
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    @Override
    public Path getDirectory() {
        return directory;
    }

}
