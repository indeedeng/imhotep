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

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class TestFlamdexMetadata {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testReadWrite() throws IOException {
        final FlamdexMetadata metadata = new FlamdexMetadata(31, ImmutableList.of("int1", "int2"), ImmutableList.of("string1", "string2"), FlamdexFormatVersion.SIMPLE);

        final Path tempDir = folder.getRoot().toPath();
        FlamdexMetadata.writeMetadata(tempDir, metadata);
        final FlamdexMetadata metadata1 = FlamdexMetadata.readMetadata(tempDir);
        assertEquals(metadata.getFormatVersion(), metadata1.getFormatVersion());
        assertEquals(metadata.getFlamdexFormatVersion(), metadata1.getFlamdexFormatVersion());
        assertEquals(metadata.getIntFields(), metadata1.getIntFields());
        assertEquals(metadata.getStringFields(), metadata1.getStringFields());
        assertEquals(metadata.getNumDocs(), metadata1.getNumDocs());
    }
}
