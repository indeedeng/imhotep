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
 package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class InputStreamFTGSIteratorTest extends TestCase {
    private void expectIntField(final FTGSIterator iter, final String field) {
        Assert.assertTrue(iter.nextField());
        Assert.assertEquals(field, iter.fieldName());
        Assert.assertTrue(iter.fieldIsIntType());
    }

    private void expectIntTerm(final FTGSIterator iter, final long term, final long termDocFreq) {
        Assert.assertTrue(iter.nextTerm());
        Assert.assertEquals(term, iter.termIntVal());
        Assert.assertEquals(termDocFreq, iter.termDocFreq());
    }

    private void expectEnd(final FTGSIterator iter) {
        expectFieldEnd(iter);
        Assert.assertFalse(iter.nextField());
    }

    private void expectFieldEnd(final FTGSIterator iter) {
        expectTermEnd(iter);
        Assert.assertFalse(iter.nextTerm());
    }

    private void expectGroup(final FTGSIterator iter, final long group, final long[] groupStats) {
        Assert.assertTrue(iter.nextGroup());
        Assert.assertEquals(group, iter.group());

        final long[] stats = new long[groupStats.length];
        iter.groupStats(stats);

        Assert.assertArrayEquals(groupStats, stats);
    }

    private void expectTermEnd(final FTGSIterator iter) {
        Assert.assertFalse(iter.nextGroup());
    }

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

    @Test
    public void testFileInputStreamIterators() throws IOException {
        final File tmp = File.createTempFile("ftgs", ".tmp");
        try {
            final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(new FileOutputStream(tmp));
            w.switchField("a", true);
            w.switchIntTerm(1, 5);
            w.switchGroup(1);
            w.addStat(15);
            w.addStat(10);

            w.switchField("b", true);

            w.switchIntTerm(2, 10);
            w.switchGroup(1);
            w.addStat(5);
            w.addStat(999999);

            w.switchIntTerm(3, 15);
            w.switchGroup(1);
            w.addStat(55);
            w.addStat(66);

            w.switchField("c", true);

            w.switchIntTerm(4, 20);
            w.switchGroup(1);
            w.addStat(1);
            w.addStat(2);

            w.close();

            {
                final InputStreamFTGSIterator iter = InputStreamFTGSIterators.create(tmp, 2);

                expectIntField(iter, "a");
                expectIntTerm(iter, 1, 5);
                expectGroup(iter, 1, new long[]{15, 10});
                expectFieldEnd(iter);

                expectIntField(iter, "b");
                expectIntTerm(iter, 2, 10);
                expectGroup(iter, 1, new long[]{5, 999999});
                expectIntTerm(iter, 3, 15);
                expectGroup(iter, 1, new long[]{55, 66});
                expectFieldEnd(iter);

                expectIntField(iter, "c");
                expectIntTerm(iter, 4, 20);
                expectGroup(iter, 1, new long[]{1, 2});
                expectFieldEnd(iter);

                expectEnd(iter);
            }

        } finally {
            tmp.delete();
        }
    }
}