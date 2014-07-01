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
