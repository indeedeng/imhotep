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
package com.indeed.flamdex.reader;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.dynamic.DynamicFlamdexReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.flamdex.ramses.RamsesFlamdexWrapper;
import com.indeed.flamdex.simple.SimpleFlamdexReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
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
        final FlamdexReader r = internalOpen(directory);
        if (RamsesFlamdexWrapper.ramsesFilesExist(directory)) {
            return new RamsesFlamdexWrapper(r, directory);
        }
        return r;
    }

    private static FlamdexReader internalOpen(final Path directory) throws IOException {
        final Path metadataPath = directory.resolve("metadata.txt");

        if (Files.notExists(directory)) {
            throw new FileNotFoundException(directory + " does not exist");
        }

        if (!Files.isDirectory(directory)) {
            throw new FileNotFoundException(directory + " is not a directory");
        }

        if (Files.notExists(metadataPath)) {
            return new LuceneFlamdexReader(directory);
        }

        final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
        switch (metadata.getFlamdexFormatVersion()) {
            case SIMPLE:
                return SimpleFlamdexReader.open(directory);
            case PFORDELTA:
                throw new UnsupportedOperationException("pfordelta is no longer supported");
            case LUCENE:
                return new LuceneFlamdexReader(directory,
                        metadata.getIntFields(),
                        metadata.getStringFields());
            case DYNAMIC:
                return new DynamicFlamdexReader(directory);
            default:
                throw new IllegalArgumentException(
                        "GenericFlamdexReader doesn't support " + metadata.getFlamdexFormatVersion().toString() + ".");
        }
    }
}
