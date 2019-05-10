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

import com.indeed.ParameterizedUtils;
import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.flamdex.api.FieldsCardinalityMetadata;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestFieldsCardinalityMetadata {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final SimpleFlamdexReader.Config config;

    @Parameterized.Parameters
    public static Iterable<SimpleFlamdexReader.Config[]> configs() {
        return ParameterizedUtils.getFlamdexConfigs();
    }

    public TestFieldsCardinalityMetadata(final SimpleFlamdexReader.Config config) {
        this.config = config;
    }

    private static void storeFlamdexToDir(final Path dir, final boolean createMetadataOnSave) throws IOException {
        try(final MockFlamdexReader reader = MakeAFlamdex.make()) {
            final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(dir, reader.getNumDocs(), true, true, createMetadataOnSave);
            SimpleFlamdexWriter.writeFlamdex(reader, writer);
        }
    }

    @Test
    public void testReadWrite() throws IOException {

        final Path flamdexDir = folder.newFolder("test-read-write").toPath();
        storeFlamdexToDir(flamdexDir, true);

        assertTrue(FieldsCardinalityMetadata.hasMetadataFile(flamdexDir));

        try (SimpleFlamdexReader simpleReader = SimpleFlamdexReader.open(flamdexDir, config)) {
            assertNotNull(simpleReader.getFieldsMetadata());
        }
    }

    @Test
    public void testCreation() throws IOException {

        final Path flamdexDir = folder.newFolder("test-creation").toPath();
        storeFlamdexToDir(flamdexDir, false);

        final SimpleFlamdexReader.Config config =
                new SimpleFlamdexReader.Config()
                        .setWriteCardinalityIfNotExisting(false);

        try (SimpleFlamdexReader simpleReader = SimpleFlamdexReader.open(flamdexDir, config)) {
            assertNull(simpleReader.getFieldsMetadata());
            simpleReader.buildAndWriteCardinalityCache(true);
            assertNotNull(simpleReader.getFieldsMetadata());
        }
    }

    @Test
    public void testCorrectness() throws IOException {

        final Path flamdexDir = folder.newFolder("test-correctness").toPath();
        storeFlamdexToDir(flamdexDir, true);

        final FieldsCardinalityMetadata metadata =
                new FieldsCardinalityMetadata.Builder()
                    .addIntField("if1", false, true, false)
                    .addIntField("if2", true, true, true)
                    .addIntField("if3", false, true, false)
                    .addStringField("sf1", true, true, false)
                    .addStringField("sf2", true, true, false)
                    .addStringField("sf3", true, true, false)
                    .addStringField("sf4", true, true, true)
                    .addStringField("floatfield", false, true, false)
                    .addStringField("uid", true, true, false)
                    .addStringField("uid_multi", true, false, true)
                    .build();

        try (SimpleFlamdexReader simpleReader = SimpleFlamdexReader.open(flamdexDir, config)) {
            assertEquals(metadata, simpleReader.getFieldsMetadata());
        }
    }
}
