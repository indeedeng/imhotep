package com.indeed.imhotep;

import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class InputStreamFTGSIteratorTest extends TestCase {
    @Test
    public void testNegative() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out);
        w.switchField("a", true);
        w.switchIntTerm(1, 5);
        w.switchGroup(1);
        w.addStat(-5);
        w.addStat(-10);
        w.switchField("b", true);
        w.switchIntTerm(1, 5);
        w.switchGroup(1);
        w.addStat(5);
        w.addStat(-999999);
        w.close();

        InputStreamFTGSIterator iter = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), 2);
        assertTrue(iter.nextField());
        assertEquals("a", iter.fieldName());
        assertTrue(iter.fieldIsIntType());
        assertTrue(iter.nextTerm());
        assertEquals(1, iter.termIntVal());
        assertEquals(5L, iter.termDocFreq());
        assertTrue(iter.nextGroup());
        assertEquals(1, iter.group());
        long[] stats = new long[2];
        iter.groupStats(stats);
        assertEquals(-5L, stats[0]);
        assertEquals(-10L, stats[1]);
        assertFalse(iter.nextGroup());
        assertFalse(iter.nextTerm());
        assertTrue(iter.nextField());
        assertEquals("b", iter.fieldName());
        assertTrue(iter.fieldIsIntType());
        assertTrue(iter.nextTerm());
        assertEquals(1, iter.termIntVal());
        assertEquals(5L, iter.termDocFreq());
        assertTrue(iter.nextGroup());
        assertEquals(1, iter.group());
        iter.groupStats(stats);
        assertEquals(5L, stats[0]);
        assertEquals(-999999L, stats[1]);
        assertFalse(iter.nextGroup());
        assertFalse(iter.nextTerm());
        assertFalse(iter.nextField());
    }

    @Test
    public void testEmptyField() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out);
        w.switchField("a", true);
        w.switchField("b", true);
        w.switchField("c", true);
        w.switchField("d", false);
        w.close();

        final InputStreamFTGSIterator iter = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), 0);
        assertTrue(iter.nextField());
        assertTrue(iter.fieldIsIntType());
        assertEquals("a", iter.fieldName());
        assertFalse(iter.nextTerm());
        assertTrue(iter.nextField());
        assertTrue(iter.fieldIsIntType());
        assertEquals("b", iter.fieldName());
        assertFalse(iter.nextTerm());
        assertTrue(iter.nextField());
        assertTrue(iter.fieldIsIntType());
        assertEquals("c", iter.fieldName());
        assertFalse(iter.nextTerm());
        assertTrue(iter.nextField());
        assertFalse(iter.fieldIsIntType());
        assertEquals("d", iter.fieldName());
        assertFalse(iter.nextTerm());
        assertFalse(iter.nextField());
    }

    @Test
    public void testIt() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        {
            final FTGSOutputStreamWriter writer = new FTGSOutputStreamWriter(out);
            writer.switchField("abc", true);
            writer.switchIntTerm(0, 5);
            writer.switchGroup(0);
            writer.addStat(12345);
            writer.addStat(0);
            writer.switchGroup(1);
            writer.addStat(2);
            writer.addStat(3);
            writer.switchGroup(4);
            writer.addStat(4);
            writer.addStat(5);
            writer.switchIntTerm(1, 4);
            writer.switchGroup(3);
            writer.addStat(6);
            writer.addStat(123456789012345l);
            writer.switchIntTerm(3, 3);
            writer.switchGroup(5);
            writer.addStat(3);
            writer.addStat(4);
            writer.switchIntTerm(100, 0); // this term shouldn't really get written
            writer.switchField("xyz", false);
            writer.switchBytesTerm("".getBytes(), "".getBytes().length, 3);
            writer.switchGroup(0);
            writer.addStat(123);
            writer.addStat(456);
            writer.switchBytesTerm("foobar".getBytes(), "foobar".getBytes().length, 2);
            writer.switchGroup(0);
            writer.addStat(999);
            writer.addStat(1000);
            writer.switchGroup(3);
            writer.addStat(1001);
            writer.addStat(1002);
            writer.switchBytesTerm("foobar2".getBytes(), "foobar2".getBytes().length, 1);
            writer.switchGroup(100);
            writer.addStat(999);
            writer.addStat(997);
            writer.switchGroup(300);
            writer.addStat(995);
            writer.addStat(993);
            writer.switchBytesTerm("foobaz".getBytes(), "foobaz".getBytes().length, 6);
            writer.switchGroup(100);
            writer.addStat(999);
            writer.addStat(997);
            writer.switchGroup(300);
            writer.addStat(995);
            writer.addStat(993);
            writer.close();
        }
        {
            final long[] stats = new long[2];
            final InputStreamFTGSIterator input = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), 2);
            assertTrue(input.nextField());
            assertEquals("abc", input.fieldName());
            assertEquals(true, input.fieldIsIntType());
            assertTrue(input.nextTerm());
            assertEquals(0, input.termIntVal());
            assertEquals(5, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(0, input.group());
            input.groupStats(stats);
            assertEquals(12345, stats[0]);
            assertEquals(0, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(1, input.group());
            input.groupStats(stats);
            assertEquals(2, stats[0]);
            assertEquals(3, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(4, input.group());
            input.groupStats(stats);
            assertEquals(4, stats[0]);
            assertEquals(5, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals(1, input.termIntVal());
            assertEquals(4, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(3, input.group());
            input.groupStats(stats);
            assertEquals(6, stats[0]);
            assertEquals(123456789012345l, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals(3, input.termIntVal());
            assertEquals(3, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(5, input.group());
            input.groupStats(stats);
            assertEquals(3, stats[0]);
            assertEquals(4, stats[1]);
            assertFalse(input.nextGroup());
            assertFalse(input.nextTerm());
            assertTrue(input.nextField());
            assertEquals("xyz", input.fieldName());
            assertEquals(false, input.fieldIsIntType());
            assertTrue(input.nextTerm());
            assertEquals("", input.termStringVal());
            assertEquals(3, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(0, input.group());
            input.groupStats(stats);
            assertEquals(123, stats[0]);
            assertEquals(456, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals("foobar", input.termStringVal());
            assertEquals(2, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(0, input.group());
            input.groupStats(stats);
            assertEquals(999, stats[0]);
            assertEquals(1000, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(3, input.group());
            input.groupStats(stats);
            assertEquals(1001, stats[0]);
            assertEquals(1002, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals("foobar2", input.termStringVal());
            assertEquals(1, input.termDocFreq());
            assertTrue(input.nextGroup());
            assertEquals(100, input.group());
            input.groupStats(stats);
            assertEquals(999, stats[0]);
            assertEquals(997, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(300, input.group());
            input.groupStats(stats);
            assertEquals(995, stats[0]);
            assertEquals(993, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals("foobaz", input.termStringVal());
            assertEquals(6, input.termDocFreq());            
            assertTrue(input.nextGroup());
            assertEquals(100, input.group());
            input.groupStats(stats);
            assertEquals(999, stats[0]);
            assertEquals(997, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(300, input.group());
            input.groupStats(stats);
            assertEquals(995, stats[0]);
            assertEquals(993, stats[1]);
            assertFalse(input.nextGroup());
            assertFalse(input.nextTerm());
            assertFalse(input.nextField());
        }
        {
            final InputStreamFTGSIterator input = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), 2);
            assertTrue(input.nextField());
            assertEquals("abc", input.fieldName());
            assertEquals(true, input.fieldIsIntType());
            assertTrue(input.nextField());
            assertEquals("xyz", input.fieldName());
            assertEquals(false, input.fieldIsIntType());
            assertFalse(input.nextField());
        }
    }
}