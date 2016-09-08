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
 package com.indeed.flamdex.simple;

import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */
public class TestSimpleIterators {
    private Path tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("flamdex-test");
    }

    @After
    public void tearDown() throws Exception {
        TestFileUtils.deleteDirTree(tempDir);
    }

    @Test
    public void testResetBeforeFirstStringTerm() throws IOException {
        SimpleFlamdexWriter writer = new SimpleFlamdexWriter(tempDir, 1L);
        final StringFieldWriter w = writer.getStringFieldWriter("stringfield");
        w.nextTerm("a");
        w.nextDoc(0);
        w.close();
        writer.close();

        final SimpleFlamdexReader reader = SimpleFlamdexReader.open(tempDir);
        final StringTermIterator iterator = reader.getStringTermIterator("stringfield");
        try {
            iterator.reset("");
            assertTrue(iterator.next());
            assertEquals("a", iterator.term());
            assertFalse(iterator.next());
        } finally {
            iterator.close();
            reader.close();
        }
    }

    @Test
    public void testResetBeforeFirstIntTerm() throws IOException {
        SimpleFlamdexWriter writer = new SimpleFlamdexWriter(tempDir, 1L);
        final IntFieldWriter w = writer.getIntFieldWriter("intfield");
        w.nextTerm(1);
        w.nextDoc(0);
        w.close();
        writer.close();

        final SimpleFlamdexReader reader = SimpleFlamdexReader.open(tempDir);
        final IntTermIterator iterator = reader.getIntTermIterator("intfield");
        try {
            iterator.reset(0);
            assertTrue(iterator.next());
            assertEquals(1, iterator.term());
            assertFalse(iterator.next());
        } finally {
            iterator.close();
            reader.close();
        }
    }
}
