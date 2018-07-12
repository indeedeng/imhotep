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
 package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
@SuppressWarnings("unused")
public abstract class AbstractFTGSMergerCase {
    @BeforeClass
    public static void initLog4j() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();

        final Layout LAYOUT = new PatternLayout("[ %d{ISO8601} %-5p ] [%c{1}] %m%n");

        final LevelRangeFilter ERROR_FILTER = new LevelRangeFilter();
        ERROR_FILTER.setLevelMin(Level.ERROR);
        ERROR_FILTER.setLevelMax(Level.FATAL);

        // everything including ERROR
        final Appender STDOUT = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_OUT);

        // just things <= ERROR
        final Appender STDERR = new ConsoleAppender(LAYOUT, ConsoleAppender.SYSTEM_ERR);
        STDERR.addFilter(ERROR_FILTER);

        final Logger ROOT_LOGGER = Logger.getRootLogger();

        ROOT_LOGGER.removeAllAppenders();

        ROOT_LOGGER.setLevel(Level.WARN); // don't care about higher

        ROOT_LOGGER.addAppender(STDOUT);
        ROOT_LOGGER.addAppender(STDERR);
    }

    protected abstract FTGSIterator newFTGSMerger(Collection<? extends FTGSIterator> iterators) throws IOException;

    @Test
    public void testEmptyFields() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out);
        w.switchField("a", true);
        w.switchField("b", true);
        w.switchField("c", true);
        w.switchField("d", false);
        w.close();

        final int n = 13;
        final List<FTGSIterator> iterators = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            final InputStream is = new ByteArrayInputStream(out.toByteArray());
            final InputStreamFTGSIterator temp = new InputStreamFTGSIterator(is, 0, 0);
            iterators.add(temp);
        }

        final FTGSIterator iter = newFTGSMerger(iterators);
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
        writeStream(out);
        {
            final int n = 13;
            final List<FTGSIterator> iterators = new ArrayList<>(n);
            final List<InputStream> inputStreams = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                final InputStream is = new ByteArrayInputStream(out.toByteArray());
                inputStreams.add(is);
                final InputStreamFTGSIterator temp = new InputStreamFTGSIterator(is, 2, 1000);
                iterators.add(temp);
            }
            final long[] stats = new long[2];

            final FTGSIterator input = newFTGSMerger(iterators);
            assertTrue(input.nextField());
            assertEquals("abc", input.fieldName());
            assertTrue(input.fieldIsIntType());
            assertTrue(input.nextTerm());
            assertEquals(0, input.termIntVal());
            assertTrue(input.nextGroup());
            assertEquals(0, input.group());
            input.groupStats(stats);
            assertEquals(12345*n, stats[0]);
            // noinspection PointlessArithmeticExpression
            assertEquals(0*n, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(1, input.group());
            input.groupStats(stats);
            assertEquals(2*n, stats[0]);
            assertEquals(3*n, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(4, input.group());
            input.groupStats(stats);
            assertEquals(4*n, stats[0]);
            assertEquals(5*n, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals(1, input.termIntVal());
            assertTrue(input.nextGroup());
            assertEquals(3, input.group());
            input.groupStats(stats);
            assertEquals(6*n, stats[0]);
            assertEquals(123456789012345L*n, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals(3, input.termIntVal());
            assertTrue(input.nextGroup());
            assertEquals(5, input.group());
            input.groupStats(stats);
            assertEquals(3*n, stats[0]);
            assertEquals(4*n, stats[1]);
            assertFalse(input.nextGroup());
            assertFalse(input.nextTerm());
            assertTrue(input.nextField());
            assertEquals("xyz", input.fieldName());
            assertFalse(input.fieldIsIntType());
            assertTrue(input.nextTerm());
            assertEquals("foobar", input.termStringVal());
            assertTrue(input.nextGroup());
            assertEquals(0, input.group());
            input.groupStats(stats);
            assertEquals(999*n, stats[0]);
            assertEquals(1000*n, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(3, input.group());
            input.groupStats(stats);
            assertEquals(1001*n, stats[0]);
            assertEquals(1002*n, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals("foobar2", input.termStringVal());
            assertTrue(input.nextGroup());
            assertEquals(100, input.group());
            input.groupStats(stats);
            assertEquals(999*n, stats[0]);
            assertEquals(997*n, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(300, input.group());
            input.groupStats(stats);
            assertEquals(995*n, stats[0]);
            assertEquals(993*n, stats[1]);
            assertFalse(input.nextGroup());
            assertTrue(input.nextTerm());
            assertEquals("foobaz", input.termStringVal());
            assertTrue(input.nextGroup());
            assertEquals(100, input.group());
            input.groupStats(stats);
            assertEquals(999*n, stats[0]);
            assertEquals(997*n, stats[1]);
            assertTrue(input.nextGroup());
            assertEquals(300, input.group());
            input.groupStats(stats);
            assertEquals(995*n, stats[0]);
            assertEquals(993*n, stats[1]);
            assertFalse(input.nextGroup());
            assertFalse(input.nextTerm());
            assertFalse(input.nextField());

            for (final InputStream is : inputStreams) {
                assertEquals(-1, is.read());
            }
        }
    }

    @Test
    public void testGroupless() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeStream(baos);
        final List<FTGSIterator> list = new ArrayList<>(10);
        for (int i = 0; i < 10; ++i) {
            final InputStream is = new ByteArrayInputStream(baos.toByteArray());
            final InputStreamFTGSIterator it = new InputStreamFTGSIterator(is, 2, 1000);
            list.add(it);
        }
        final FTGSIterator it = newFTGSMerger(list);
        assertTrue(it.nextField());
        assertEquals("abc", it.fieldName());
        assertTrue(it.fieldIsIntType());
        assertTrue(it.nextTerm());
        assertEquals(0, it.termIntVal());
        assertTrue(it.nextTerm());
        assertEquals(1, it.termIntVal());
        assertTrue(it.nextTerm());
        assertEquals(3, it.termIntVal());
        assertFalse(it.nextTerm());
        assertTrue(it.nextField());
        assertEquals("xyz", it.fieldName());
        assertFalse(it.fieldIsIntType());
        assertTrue(it.nextTerm());
        assertEquals("foobar", it.termStringVal());
        assertTrue(it.nextTerm());
        assertEquals("foobar2", it.termStringVal());
        assertTrue(it.nextTerm());
        assertEquals("foobaz", it.termStringVal());
        assertFalse(it.nextTerm());
        assertFalse(it.nextField());
    }

    private static void writeStream(final ByteArrayOutputStream out) throws IOException {
        final FTGSOutputStreamWriter writer = new FTGSOutputStreamWriter(out);
        writer.switchField("abc", true);
        writer.switchIntTerm(0, 0);
        writer.switchGroup(0);
        writer.addStat(12345);
        writer.addStat(0);
        writer.switchGroup(1);
        writer.addStat(2);
        writer.addStat(3);
        writer.switchGroup(4);
        writer.addStat(4);
        writer.addStat(5);
        writer.switchIntTerm(1, 0);
        writer.switchGroup(3);
        writer.addStat(6);
        writer.addStat(123456789012345L);
        writer.switchIntTerm(3, 0);
        writer.switchGroup(5);
        writer.addStat(3);
        writer.addStat(4);
        writer.switchIntTerm(100, 0); // this term shouldn't really get written
        writer.switchField("xyz", false);
        writer.switchBytesTerm("foobar".getBytes(), "foobar".getBytes().length, 0);
        writer.switchGroup(0);
        writer.addStat(999);
        writer.addStat(1000);
        writer.switchGroup(3);
        writer.addStat(1001);
        writer.addStat(1002);
        writer.switchBytesTerm("foobar2".getBytes(), "foobar2".getBytes().length, 0);
        writer.switchGroup(100);
        writer.addStat(999);
        writer.addStat(997);
        writer.switchGroup(300);
        writer.addStat(995);
        writer.addStat(993);
        writer.switchBytesTerm("foobaz".getBytes(), "foobaz".getBytes().length, 0);
        writer.switchGroup(100);
        writer.addStat(999);
        writer.addStat(997);
        writer.switchGroup(300);
        writer.addStat(995);
        writer.addStat(993);
        writer.close();
    }

    @Test
    public void testSomethingElse() throws IOException {
        final List<ByteArrayOutputStream> streams = new ArrayList<>();
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            streams.add(baos);
            final FTGSOutputStreamWriter osw = new FTGSOutputStreamWriter(baos);
            osw.switchField("companyid", true);
            osw.switchIntTerm(5, 10);
            osw.switchGroup(1);
            osw.addStat(10);
            osw.addStat(11);
            osw.switchGroup(2);
            osw.addStat(12);
            osw.addStat(13);
            osw.switchGroup(4);
            osw.addStat(14);
            osw.addStat(15);
            osw.switchIntTerm(10, 20);
            osw.switchGroup(1);
            osw.addStat(16);
            osw.addStat(17);
            osw.close();
            baos.close();
        }
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            streams.add(baos);
            final FTGSOutputStreamWriter osw = new FTGSOutputStreamWriter(baos);
            osw.switchField("companyid", true);
            osw.switchIntTerm(5, 10);
            osw.switchGroup(2);
            osw.addStat(10);
            osw.addStat(11);
            osw.switchGroup(3);
            osw.addStat(12);
            osw.addStat(13);
            osw.switchGroup(5);
            osw.addStat(14);
            osw.addStat(15);
            osw.switchIntTerm(10, 20);
            osw.switchGroup(4);
            osw.addStat(16);
            osw.addStat(17);
            osw.close();
            baos.close();
        }
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            streams.add(baos);
            final FTGSOutputStreamWriter osw = new FTGSOutputStreamWriter(baos);
            osw.switchField("companyid", true);
            osw.switchIntTerm(5, 10);
            osw.switchGroup(1);
            osw.addStat(10);
            osw.addStat(11);
            osw.switchGroup(2);
            osw.addStat(12);
            osw.addStat(13);
            osw.switchGroup(4);
            osw.addStat(14);
            osw.addStat(15);
            osw.switchIntTerm(9, 20);
            osw.switchGroup(3);
            osw.addStat(16);
            osw.addStat(17);
            osw.close();
            baos.close();
        }

        final List<FTGSIterator> iterators = new ArrayList<>();
        for (final ByteArrayOutputStream os : streams) {
            iterators.add(new InputStreamFTGSIterator(new ByteArrayInputStream(os.toByteArray()), 2, 10));
        }
        final FTGSIterator merger = newFTGSMerger(iterators);
        final long[] stats = new long[2];

        assertTrue(merger.nextField());
        assertEquals("companyid", merger.fieldName());
        assertTrue(merger.fieldIsIntType());
        assertTrue(merger.nextTerm());
        assertEquals(5, merger.termIntVal());
        assertEquals(30, merger.termDocFreq());
        assertTrue(merger.nextGroup());
        assertEquals(1, merger.group());
        merger.groupStats(stats);
        assertEquals(20L, stats[0]);
        assertEquals(22L, stats[1]);
        assertTrue(merger.nextGroup());
        assertEquals(2, merger.group());
        merger.groupStats(stats);
        assertEquals(34L, stats[0]);
        assertEquals(37L, stats[1]);
        assertTrue(merger.nextGroup());
        assertEquals(3, merger.group());
        merger.groupStats(stats);
        assertEquals(12L, stats[0]);
        assertEquals(13L, stats[1]);
        assertTrue(merger.nextGroup());
        assertEquals(4, merger.group());
        merger.groupStats(stats);
        assertEquals(28L, stats[0]);
        assertEquals(30L, stats[1]);
        assertTrue(merger.nextGroup());
        assertEquals(5, merger.group());
        merger.groupStats(stats);
        assertEquals(14L, stats[0]);
        assertEquals(15L, stats[1]);
        assertFalse(merger.nextGroup());
        assertTrue(merger.nextTerm());
        assertEquals(9, merger.termIntVal());
        assertEquals(20, merger.termDocFreq());
        assertTrue(merger.nextGroup());
        assertEquals(3, merger.group());
        merger.groupStats(stats);
        assertEquals(16L, stats[0]);
        assertEquals(17L, stats[1]);
        assertFalse(merger.nextGroup());
        assertTrue(merger.nextTerm());
        assertEquals(10, merger.termIntVal());
        assertEquals(40, merger.termDocFreq());
        assertTrue(merger.nextGroup());
        assertEquals(1, merger.group());
        merger.groupStats(stats);
        assertEquals(16L, stats[0]);
        assertEquals(17L, stats[1]);
        assertTrue(merger.nextGroup());
        assertEquals(4, merger.group());
        merger.groupStats(stats);
        assertEquals(16L, stats[0]);
        assertEquals(17L, stats[1]);
        assertFalse(merger.nextGroup());
        assertFalse(merger.nextTerm());
        assertFalse(merger.nextField());
    }

    @Test
    public void testTermWithNoGroups() throws IOException {
        final FTGSIterator iterator = newFTGSMerger(Lists.newArrayList(makeMockIterator(0), makeMockIterator(0)));
        assertTrue(iterator.nextField());
        assertFalse(iterator.fieldIsIntType());
        assertTrue(iterator.nextTerm());
        assertEquals("a", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(1, iterator.group());
        assertTrue(iterator.nextGroup());
        assertEquals(2, iterator.group());
        assertTrue(iterator.nextGroup());
        assertEquals(3, iterator.group());
        assertTrue(iterator.nextGroup());
        assertEquals(4, iterator.group());
        assertTrue(iterator.nextGroup());
        assertEquals(5, iterator.group());
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());
        assertEquals("b", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(1, iterator.group());
        assertTrue(iterator.nextGroup());
        assertEquals(3, iterator.group());
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());
        assertEquals("c", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(4, iterator.group());
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());

        // it's ok if "d" is skipped since it matches no groups
        if ("d".equals(iterator.termStringVal())) {
            assertFalse(iterator.nextGroup());
            assertTrue(iterator.nextTerm());
        }
        
        assertEquals("e", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(5, iterator.group());
        assertFalse(iterator.nextGroup());
        assertFalse(iterator.nextTerm());
        assertFalse(iterator.nextField());
    }

    @Test
    public void testTestTest() throws IOException {
        final FTGSIterator iterator = newFTGSMerger(Lists.newArrayList(makeMockIterator3(), makeMockIterator2()));
        final long[] stats = new long[2];
        assertTrue(iterator.nextField());
        assertFalse(iterator.fieldIsIntType());
        assertTrue(iterator.nextTerm());
        assertEquals("a", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(1, iterator.group());
        iterator.groupStats(stats);
        assertEquals(18, stats[0]);
        assertEquals(20, stats[1]);
        assertTrue(iterator.nextGroup());
        assertEquals(2, iterator.group());
        iterator.groupStats(stats);
        assertEquals(22, stats[0]);
        assertEquals(24, stats[1]);
        assertTrue(iterator.nextGroup());
        assertEquals(3, iterator.group());
        iterator.groupStats(stats);
        assertEquals(26, stats[0]);
        assertEquals(28, stats[1]);
        assertTrue(iterator.nextGroup());
        assertEquals(4, iterator.group());
        iterator.groupStats(stats);
        assertEquals(30, stats[0]);
        assertEquals(32, stats[1]);
        assertTrue(iterator.nextGroup());
        assertEquals(5, iterator.group());
        iterator.groupStats(stats);
        assertEquals(34, stats[0]);
        assertEquals(36, stats[1]);
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());
        assertEquals("b", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(1, iterator.group());
        iterator.groupStats(stats);
        assertEquals(38, stats[0]);
        assertEquals(40, stats[1]);
        assertTrue(iterator.nextGroup());
        assertEquals(3, iterator.group());
        iterator.groupStats(stats);
        assertEquals(42, stats[0]);
        assertEquals(44, stats[1]);
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());
        assertEquals("c", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        assertEquals(4, iterator.group());
        iterator.groupStats(stats);
        assertEquals(46, stats[0]);
        assertEquals(48, stats[1]);
        assertFalse(iterator.nextGroup());
        assertTrue(iterator.nextTerm());

        // it's ok if "d" is skipped since it matches no groups
        if ("d".equals(iterator.termStringVal())) {
            assertFalse(iterator.nextGroup());
            assertTrue(iterator.nextTerm());
        }

        // it's ok if "e" is skipped since it matches no groups        
        if ("e".equals(iterator.termStringVal())) {
            assertFalse(iterator.nextGroup());
            assertTrue(iterator.nextTerm());
        }

        assertEquals("f", iterator.termStringVal());
        assertTrue(iterator.nextGroup());
        iterator.groupStats(stats);
        assertEquals(100, stats[0]);
        assertEquals(101, stats[1]);
        assertFalse(iterator.nextTerm());
        assertFalse(iterator.nextField());
    }

    private static FTGSIterator makeMockIterator(final int numStats) {
        return new FTGSIterator() {
            final String[] fields = {"f1"};
            final boolean[] intType = {false};
            int fieldIndex = -1;
            final String[][] stringVals = {{"a", "b", "c", "d", "e"}};
            int termIndex = -1;
            final int[][][] groups = {{
                    {1, 2, 3, 4, 5},
                    {1, 3},
                    {4},
                    {},
                    {5}
            }};
            final long[][][][] groupStats = {{
                    {{17, 18}, {19, 20}, {21, 22}, {23, 24}, {25, 26}},
                    {{27, 28}, {29, 30}},
                    {{31, 32}},
                    {},
                    {{33, 34}}
            }};
            int groupIndex = -1;

            @Override
            public int getNumStats() {
                return numStats;
            }

            @Override
            public int getNumGroups() {
                return 6;
            }

            @Override
            public boolean nextField() {
                if (++fieldIndex >= fields.length) {
                    return false;
                }
                termIndex = -1;
                return true;
            }

            @Override
            public String fieldName() {
                return fields[fieldIndex];
            }

            @Override
            public boolean fieldIsIntType() {
                return intType[fieldIndex];
            }

            @Override
            public boolean nextTerm() {
                if (++termIndex >= stringVals[fieldIndex].length) {
                    return false;
                }
                groupIndex = -1;
                return true;
            }

            @Override
            public long termDocFreq() {
                return 0L;
            }

            @Override
            public long termIntVal() {
                return 0;
            }

            @Override
            public String termStringVal() {
                return stringVals[fieldIndex][termIndex];
            }

            @Override
            public byte[] termStringBytes() {
                return termStringVal().getBytes(Charsets.UTF_8);
            }

            @Override
            public int termStringLength() {
                return termStringBytes().length;
            }

            @Override
            public boolean nextGroup() {
                return ++groupIndex < groups[fieldIndex][termIndex].length;
            }

            @Override
            public int group() {
                return groups[fieldIndex][termIndex][groupIndex];
            }

            @Override
            public void groupStats(final long[] stats) {
                System.arraycopy(groupStats[fieldIndex][termIndex][groupIndex], 0, stats, 0, numStats);
            }

            @Override
            public void close() {
            }
        };
    }

    private static FTGSIterator makeMockIterator2() {
        return new FTGSIterator() {
            final String[] fields = {"f1"};
            final boolean[] intType = {false};
            int fieldIndex = -1;
            final String[][] stringVals = {{"a", "b", "c", "d", "e", "f"}};
            int termIndex = -1;
            final int[][][] groups = {{
                    {1, 2, 3, 4, 5},
                    {1, 3},
                    {4},
                    {},
                    {},
                    {1}
            }};
            final long[][][][] groupStats = {{
                    {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}},
                    {{11, 12}, {13, 14}},
                    {{15, 16}},
                    {},
                    {},
                    {{100, 101}}
            }};
            int groupIndex = -1;

            @Override
            public int getNumStats() {
                return 2;
            }

            @Override
            public int getNumGroups() {
                return 6;
            }

            @Override
            public boolean nextField() {
                if (++fieldIndex >= fields.length) {
                    return false;
                }
                termIndex = -1;
                return true;
            }

            @Override
            public String fieldName() {
                return fields[fieldIndex];
            }

            @Override
            public boolean fieldIsIntType() {
                return intType[fieldIndex];
            }

            @Override
            public boolean nextTerm() {
                if (++termIndex >= stringVals[fieldIndex].length) {
                    return false;
                }
                groupIndex = -1;
                return true;
            }

            @Override
            public long termDocFreq() {
                return 0L;
            }

            @Override
            public long termIntVal() {
                return 0;
            }

            @Override
            public String termStringVal() {
                return stringVals[fieldIndex][termIndex];
            }

            @Override
            public byte[] termStringBytes() {
                return termStringVal().getBytes(Charsets.UTF_8);
            }

            @Override
            public int termStringLength() {
                return termStringBytes().length;
            }

            @Override
            public boolean nextGroup() {
                return ++groupIndex < groups[fieldIndex][termIndex].length;
            }

            @Override
            public int group() {
                return groups[fieldIndex][termIndex][groupIndex];
            }

            @Override
            public void groupStats(final long[] stats) {
                System.arraycopy(groupStats[fieldIndex][termIndex][groupIndex], 0, stats, 0, 2);
            }

            @Override
            public void close() {
            }
        };
    }

    private static FTGSIterator makeMockIterator3() {
        return new FTGSIterator() {
            final String[] fields = {"f1"};
            final boolean[] intType = {false};
            int fieldIndex = -1;
            final String[][] stringVals = {{"a", "b", "c", "d"}};
            int termIndex = -1;
            final int[][][] groups = {{
                    {1, 2, 3, 4, 5},
                    {1, 3},
                    {4},
                    {},
            }};
            final long[][][][] groupStats = {{
                    {{17, 18}, {19, 20}, {21, 22}, {23, 24}, {25, 26}},
                    {{27, 28}, {29, 30}},
                    {{31, 32}},
                    {},
            }};
            int groupIndex = -1;

            @Override
            public int getNumStats() {
                return 2;
            }

            @Override
            public int getNumGroups() {
                return 6;
            }

            @Override
            public boolean nextField() {
                if (++fieldIndex >= fields.length) {
                    return false;
                }
                termIndex = -1;
                return true;
            }

            @Override
            public String fieldName() {
                return fields[fieldIndex];
            }

            @Override
            public boolean fieldIsIntType() {
                return intType[fieldIndex];
            }

            @Override
            public boolean nextTerm() {
                if (++termIndex >= stringVals[fieldIndex].length) {
                    return false;
                }
                groupIndex = -1;
                return true;
            }

            @Override
            public long termDocFreq() {
                return 0L;
            }

            @Override
            public long termIntVal() {
                return 0;
            }

            @Override
            public String termStringVal() {
                return stringVals[fieldIndex][termIndex];
            }

            @Override
            public byte[] termStringBytes() {
                return termStringVal().getBytes(Charsets.UTF_8);
            }

            @Override
            public int termStringLength() {
                return termStringBytes().length;
            }

            @Override
            public boolean nextGroup() {
                return ++groupIndex < groups[fieldIndex][termIndex].length;
            }

            @Override
            public int group() {
                return groups[fieldIndex][termIndex][groupIndex];
            }

            @Override
            public void groupStats(final long[] stats) {
                System.arraycopy(groupStats[fieldIndex][termIndex][groupIndex], 0, stats, 0, 2);
            }

            @Override
            public void close() {
            }
        };
    }
}
