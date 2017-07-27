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
 package com.indeed.imhotep.archive;

import org.junit.Test;

import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 *
 * code coverage!
 */
public class TestFileMetadata {
    @Test
    public void testEqualsHashCode() {
        final FileMetadata fm = new FileMetadata("a", 0, 0, "abcdef0123456789abcdef0123456789", 0, NONE, "a");
        assertTrue(fm.equals(fm));
        assertFalse(fm.equals(new Object()));
        assertFalse(fm.equals(new FileMetadata("a", 0, 0, "abcdef0123456789abcdef0123456789", 0, NONE, "b")));

        assertEquals(fm.hashCode(), fm.hashCode());
        assertEquals(fm.toString(), fm.toString());

        final FileMetadata fm3 = new FileMetadata("a", 0, 0, "abcdef0123456789abcdef0123456789", 0, NONE, "a");
        assertTrue(fm3.equals(fm));
    }
}
