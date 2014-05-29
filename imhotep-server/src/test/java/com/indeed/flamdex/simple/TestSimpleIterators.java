package com.indeed.flamdex.simple;

import com.indeed.util.io.Files;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */
public class TestSimpleIterators {
    private String tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.getTempDirectory("flamdex-test", "dir");
    }

    @After
    public void tearDown() throws Exception {
        Files.delete(tempDir);
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
