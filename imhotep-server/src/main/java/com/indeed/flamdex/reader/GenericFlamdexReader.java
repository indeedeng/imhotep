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
package com.indeed.flamdex.reader;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.dynamic.DynamicFlamdexReader;
import com.indeed.flamdex.ramses.RamsesFlamdexWrapper;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author jplaisance
 */

// This class was an implementation of FlamdexReader,
// But now it is used only for FlamdexReader opening,
// so all except opening is deleted.
public class GenericFlamdexReader {
    
    private GenericFlamdexReader() {
    }

    public static FlamdexReader open(final Path directory) throws IOException {
        return open(directory, -1);
    }

    public static FlamdexReader open(final Path directory, final int numDocs) throws IOException {
        final FlamdexFormatVersion formatVersion = getFormatVersionFromDirectory(directory);
        final FlamdexReader r = numDocs < 0 ?
                internalOpen(directory, formatVersion) :
                internalOpen(directory, formatVersion, numDocs);

        if (RamsesFlamdexWrapper.ramsesFilesExist(directory)) {
            return new RamsesFlamdexWrapper(r, directory);
        }
        return r;
    }

    private static FlamdexReader internalOpen(final Path directory, final FlamdexFormatVersion formatVersion) throws IOException {
        return formatVersion == FlamdexFormatVersion.SIMPLE ?
                SimpleFlamdexReader.open(directory) :
                new DynamicFlamdexReader(directory);
    }

    private static FlamdexReader internalOpen(final Path directory, final FlamdexFormatVersion formatVersion, final int numDocs) throws IOException {
        return formatVersion == FlamdexFormatVersion.SIMPLE ?
                SimpleFlamdexReader.open(directory, numDocs) :
                new DynamicFlamdexReader(directory);
    }

    private static FlamdexFormatVersion getFormatVersionFromDirectory(final Path directory) {
        final String dirWithoutSuffix = StringUtils.removeEnd(directory.getFileName().toString(), ".sqar");
        return DynamicIndexSubshardDirnameUtil.isValidDynamicIndexName(dirWithoutSuffix) ?
                FlamdexFormatVersion.DYNAMIC :
                FlamdexFormatVersion.SIMPLE;
    }
}
