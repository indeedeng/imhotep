package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jwolfe
 */
public class MultiSessionMergerTest {
    @Test
    public void smokeTest() throws Exception {
        final ByteArrayOutputStream os1 = new ByteArrayOutputStream();
        try (FTGSOutputStreamWriter out1 = new FTGSOutputStreamWriter(os1)) {
            out1.switchField("field1", false);
            out1.switchBytesTerm("abc".getBytes(), 3, 5);
            out1.switchGroup(1);
            out1.addStat(5);
            out1.addStat(0);
            out1.switchGroup(2);
            out1.addStat(1);
            out1.addStat(1);

            out1.switchBytesTerm("bar".getBytes(), 3, 10);
            out1.switchGroup(2);
            out1.addStat(100);
            out1.addStat(5);
            out1.switchGroup(3);
            out1.addStat(12);
            out1.addStat(-100);

            out1.switchBytesTerm("z".getBytes(), 1, 100);
            out1.switchGroup(2);
            out1.addStat(100);
            out1.addStat(5);
        }

        final ByteArrayOutputStream os2 = new ByteArrayOutputStream();
        try (FTGSOutputStreamWriter out2 = new FTGSOutputStreamWriter(os2)) {
            out2.switchField("field2", false);

            out2.switchBytesTerm("a".getBytes(), 1, 10);
            out2.switchGroup(1);
            out2.addStat(1);

            out2.switchBytesTerm("abc".getBytes(), 3, 100);
            out2.switchGroup(1);
            out2.addStat(20);

            out2.switchBytesTerm("bar".getBytes(), 3, 1000);
            out2.switchGroup(1);
            out2.addStat(0);
            out2.switchGroup(3);
            out2.addStat(-100);
        }

        final long[] statsBuf1 = new long[2];
        final long[] statsBuf2 = new long[1];

        try (
                InputStreamFTGSIterator in1 = new InputStreamFTGSIterator(new ByteArrayInputStream(os1.toByteArray()), 2, 3);
                InputStreamFTGSIterator in2 = new InputStreamFTGSIterator(new ByteArrayInputStream(os2.toByteArray()), 1, 3);
                MultiSessionMerger merger = new MultiSessionMerger(Lists.newArrayList(in1, in2), null)
        ) {
            assertTrue(merger.nextField());

            //
            // a
            //

            assertTrue(merger.nextTerm());
            assertEquals("a", merger.termStringVal());
            assertTrue(merger.nextGroup());
            assertEquals(1, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 0, 0 }, statsBuf1);
            assertArrayEquals(new long[]{ 1 }, statsBuf2);
            assertFalse(merger.nextGroup());

            //
            // abc
            //

            assertTrue(merger.nextTerm());
            assertEquals("abc", merger.termStringVal());

            assertTrue(merger.nextGroup());
            assertEquals(1, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 5, 0 }, statsBuf1);
            assertArrayEquals(new long[]{ 20 }, statsBuf2);

            assertTrue(merger.nextGroup());
            assertEquals(2, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 1, 1 }, statsBuf1);
            assertArrayEquals(new long[]{ 0 }, statsBuf2);

            //
            // bar
            //

            assertTrue(merger.nextTerm());
            assertEquals("bar", merger.termStringVal());

            assertTrue(merger.nextGroup());
            assertEquals(1, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 0, 0 }, statsBuf1);
            assertArrayEquals(new long[]{ 0 }, statsBuf2);

            assertTrue(merger.nextGroup());
            assertEquals(2, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 100, 5 }, statsBuf1);
            assertArrayEquals(new long[]{ 0 }, statsBuf2);

            assertTrue(merger.nextGroup());
            assertEquals(3, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 12, -100 }, statsBuf1);
            assertArrayEquals(new long[]{ -100 }, statsBuf2);

            //
            // z
            //

            assertTrue(merger.nextTerm());
            assertEquals("z", merger.termStringVal());

            assertTrue(merger.nextGroup());
            assertEquals(2, merger.group());
            merger.groupStats(0, statsBuf1);
            merger.groupStats(1, statsBuf2);
            assertArrayEquals(new long[]{ 100, 5 }, statsBuf1);
            assertArrayEquals(new long[]{ 0 }, statsBuf2);
        }
    }
}