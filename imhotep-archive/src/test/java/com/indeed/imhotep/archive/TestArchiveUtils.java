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
 package com.indeed.imhotep.archive;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author jsgroth
 */
public class TestArchiveUtils {

    @Test
    public void testStreamCopy() throws IOException {
        for (int i = 0; i < 10; ++i) {
            final InputStream is = new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            ArchiveUtils.streamCopy(is, os, i);
            os.close();
            final byte[] bytes = os.toByteArray();
            assertEquals(i, bytes.length);
            for (int j = 0; j < i; ++j) {
                assertEquals(j + 1, bytes[j]);
            }
        }
    }
}
