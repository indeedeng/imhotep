package com.indeed.imhotep.archive;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.indeed.imhotep.archive.ArchiveUtils;

/**
 * @author jsgroth
 */
public class TestArchiveUtils extends TestCase {
    public void testStreamCopy() throws IOException {
        for (int i = 0; i < 10; ++i) {
            InputStream is = new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ArchiveUtils.streamCopy(is, os, i);
            os.close();
            byte[] bytes = os.toByteArray();
            assertEquals(i, bytes.length);
            for (int j = 0; j < i; ++j) {
                assertEquals(j + 1, bytes[j]);
            }
        }
    }
}
