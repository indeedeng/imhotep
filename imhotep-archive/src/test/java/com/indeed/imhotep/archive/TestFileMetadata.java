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

import junit.framework.TestCase;

import org.junit.Test;

import com.indeed.imhotep.archive.FileMetadata;

import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.NONE;

/**
 * @author jsgroth
 *
 * code coverage!
 */
public class TestFileMetadata extends TestCase {
    @Test
    public void testEqualsHashCode() {
        FileMetadata fm = new FileMetadata("a", 0, 0, "a", 0, NONE, "a");
        assertTrue(fm.equals(fm));
        assertFalse(fm.equals(new Object()));
        assertFalse(fm.equals(new FileMetadata("a", 0, 0, "a", 0, NONE, "b")));

        assertEquals(fm.hashCode(), fm.hashCode());
        assertEquals(fm.toString(), fm.toString());

        FileMetadata fm3 = new FileMetadata("a", 0, 0, "a", 0, NONE, "a");
        assertTrue(fm3.equals(fm));
    }
}
