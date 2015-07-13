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
 package com.indeed.imhotep.local;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.BucketStats;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.imhotep.group.ImhotepChooser;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.*;

/**
 * @author jsgroth
 */
public class TestImhotepLocalSession {
    @Test
    public void testPushPopGetDepth() throws ImhotepOutOfMemoryException {
        // This test doesn't really specifically need the 2d test setup,
        // but that setup is good enoguh for this too.
        FlamdexReader r = new2DMetricRegroupTestReader();
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        assertEquals(0, session.getNumStats());
        int numStats = session.pushStat("if1");
        assertEquals(1, numStats);
        assertEquals(1, session.getNumStats());
        numStats = session.pushStat("if2");
        assertEquals(2, numStats);
        assertEquals(2, session.getNumStats());
        numStats = session.pushStat("if3");
        assertEquals(3, numStats);
        assertEquals(3, session.getNumStats());

        numStats = session.popStat(); // should pop "if3"
        assertEquals(2, numStats);
        assertEquals(2, session.getNumStats());

        numStats = session.pushStat("*"); // should reduce to if1 * if2
        assertEquals(1, numStats);
        assertEquals(1, session.getNumStats());

        numStats = session.pushStats(Arrays.asList("if1", "if2", "if3"));
        assertEquals(4, numStats);
        assertEquals(4, session.getNumStats());
    }

    @Test
    public void testMoreConditionsThanTargetGroups() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 16);
        r.addIntTerm("if1", 1, 1, 3, 5, 7, 9, 11, 13, 15); // 0th bit
        r.addIntTerm("if1", 2, 2, 3, 6, 7, 10, 11, 14, 15); // 1st bit
        r.addIntTerm("if1", 4, 4, 5, 6, 7, 12, 13, 14, 15); // 2nd bit
        r.addIntTerm("if1", 8, 8, 9, 10, 11, 12, 13, 14, 15); // 2nd bit
        // 0000, 0001, 0010, 0011, 0100, 0101, 0110, 0111, 1000, 1001, ...
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            0,
                                                                            new int[] { 1, 1, 1, 1 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         1,
                                                                                                                         null,
                                                                                                                         false),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         2,
                                                                                                                         null,
                                                                                                                         false),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         4,
                                                                                                                         null,
                                                                                                                         false),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         8,
                                                                                                                         null,
                                                                                                                         false), }) });
        int[] arr = new int[16];
        session.exportDocIdToGroupId(arr);
        System.out.println(Arrays.toString(arr));
        assertArrayEquals(new int[] { 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }, arr);
    }

    @Test
    public void testResetThenRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Collections.<String> emptyList(),
                                      Arrays.asList("if1"), 10);
        r.addIntTerm("if1", 0, 1, 3, 5, 7, 9);
        r.addIntTerm("if1", 5, 0, 2, 4, 6, 8);

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupRemapRule[] { new GroupRemapRule(
                                                                  1,
                                                                  new RegroupCondition("if1", true,
                                                                                       0, "", false),
                                                                  0, 1) });
        session.resetGroups();
        session.pushStat("count()");
        int numGroups =
                session.regroup(new GroupRemapRule[] { new GroupRemapRule(
                                                                          99999,
                                                                          new RegroupCondition(
                                                                                               "if1",
                                                                                               true,
                                                                                               1,
                                                                                               "",
                                                                                               false),
                                                                          1, 1) });
        assertEquals(1, numGroups);
    }

    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r = newMetricRegroupTestReader();

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("if1");
        int numGroups = session.metricRegroup(0, 0, 20, 5);
        assertEquals(7, numGroups); // 4 buckets, 2 gutters, group 0

        int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        assertEquals(Arrays.asList(2, 3, 2, 4, 2, 4, 2, 3, 2, 6), Ints.asList(docIdToGroup));
    }

    @Test
    public void testMetricRegroup2() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r = newMetricRegroupTestReader();

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        assertEquals(2,
                     session.regroup(new GroupRemapRule[] { new GroupRemapRule(
                                                                               1,
                                                                               new RegroupCondition(
                                                                                                    "sf1",
                                                                                                    false,
                                                                                                    0,
                                                                                                    "☃",
                                                                                                    false),
                                                                               1, 0) }));

        session.pushStat("if1");
        int numGroups = session.metricRegroup(0, 9, 17, 4);
        assertEquals(5, numGroups); // 2 buckets, 2 gutters, group 0

        int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        assertEquals(Arrays.asList(3, 1, 3, 0, 0, 2, 3, 1, 0, 4), Ints.asList(docIdToGroup));
    }

    private static MockFlamdexReader newMetricRegroupTestReader() {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.asList("sf1"),
                                      Arrays.asList("if1"), 10);
        r.addIntTerm("if1", 5, Arrays.asList(0, 2, 4, 6, 8));
        r.addIntTerm("if1", 10, Arrays.asList(1, 7));
        r.addIntTerm("if1", 15, Arrays.asList(3, 5));
        r.addIntTerm("if1", 20, Arrays.asList(9));
        r.addStringTerm("sf1", "☃", Arrays.asList(3, 4, 8));
        return r;
    }

    @Test
    public void test2DMetricRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r = new2DMetricRegroupTestReader();
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("if1");
        session.pushStat("if2");
        session.metricRegroup2D(0, 1, 8, 3, 1, 4, 12, 2);

        int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        assertEquals(Arrays.asList(1, 2, 7, 7, 13, 13, 18, 19, 25, 25), Ints.asList(docIdToGroup));

        session.pushStat("if3");

        long[] if3 = Arrays.copyOf(session.getGroupStats(2), 31);
        long[] expected = { 0, 1, 4, 0, 0, 0, // 1-5
                           0, 25, 0, 0, 0, // 6-10
                           0, 0, 61, 0, 0, // 11-15
                           0, 0, 49, 64, 0, // 16-20
                           0, 0, 0, 0, 181, // 21-25
                           0, 0, 0, 0, 0, // 26-30
        };
        assertEquals(Longs.asList(expected), Longs.asList(if3));

        BucketStats bs = new BucketStats(if3, 5, 6);
        assertEquals(25, bs.get(0, 0));
        assertEquals(1, bs.getXYUnderflow());
        assertEquals(4, bs.getYUnderflow(0));
        assertEquals(61, bs.get(1, 1));
        assertEquals(49, bs.get(1, 2));
        assertEquals(64, bs.get(2, 2));
        assertEquals(181, bs.getXOverflow(3));
        for (int y = 0; y < 4; ++y) {
            assertEquals(0, bs.getXUnderflow(y));
        }

        for (int x = 0; x < 3; ++x) {
            assertEquals(0, bs.getYOverflow(x));
        }

        assertEquals(0, bs.getXYOverflow());
        assertEquals(0, bs.getXUnderflowYOverflow());
        assertEquals(0, bs.getXOverflowYUnderflow());
        assertEquals(0, bs.getYUnderflow(1));
        assertEquals(0, bs.getYUnderflow(2));
        assertEquals(0, bs.get(1, 0));
        assertEquals(0, bs.get(2, 0));
        assertEquals(0, bs.get(0, 1));
        assertEquals(0, bs.get(0, 2));
        assertEquals(0, bs.get(0, 3));
        assertEquals(0, bs.get(2, 1));
        assertEquals(0, bs.get(2, 3));
        assertEquals(0, bs.get(1, 3));
        for (int y = 0; y < 3; ++y) {
            assertEquals(0, bs.getXOverflow(y));
        }
    }

    @Test
    public void test2DMetricRegroup2() throws ImhotepOutOfMemoryException {
        FlamdexReader r = new2DMetricRegroupTestReader();
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("if1");
        session.pushStat("if2");

        session.metricRegroup2D(0, 4, 8, 2, 1, 2, 4, 2);

        int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        assertEquals(Ints.asList(5, 5, 9, 9, 10, 10, 11, 11, 12, 12), Ints.asList(docIdToGroup));

        session.pushStat("if3");

        long[] if3 = Arrays.copyOf(session.getGroupStats(2), 13);
        long[] expected = { 0, 0, 0, 0, 0, // 1-4
                           5, 0, 0, 0, // 5-8
                           25, 61, 113, 181, // 9-12
        };

        assertEquals(Longs.asList(expected), Longs.asList(if3));

        BucketStats bs = new BucketStats(if3, 4, 3);
        assertEquals(5, bs.getXUnderflow(0));
        assertEquals(25, bs.getXUnderflowYOverflow());
        assertEquals(61, bs.getYOverflow(0));
        assertEquals(113, bs.getYOverflow(1));
        assertEquals(181, bs.getXYOverflow());

        assertEquals(0, bs.getXYUnderflow());
        assertEquals(0, bs.getYUnderflow(0));
        assertEquals(0, bs.getYUnderflow(1));
        assertEquals(0, bs.getXOverflowYUnderflow());
        assertEquals(0, bs.get(0, 0));
        assertEquals(0, bs.get(1, 0));
    }

    @Test
    public void test2DMetricRegroup3() throws ImhotepOutOfMemoryException {
        FlamdexReader r = new2DMetricRegroupTestReader();
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("if1");
        session.pushStat("if2");

        session.regroup(new QueryRemapRule(1, Query.newTermQuery(new Term("sf1", false, 0, "☃")),
                                           1, 0));

        session.metricRegroup2D(0, 0, 1, 90000, 1, 8, 10, 2);

        int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        assertEquals(Ints.asList(2, 0, 3, 3, 0, 0, 0, 0, 9, 9), Ints.asList(docIdToGroup));

        session.pushStat("if3");
        long[] if3 = Arrays.copyOf(session.getGroupStats(2), 10);
        long[] expected = { 0, 0, 1, 25, 0, 0, 0, 0, 0, 181, };

        assertEquals(Longs.asList(expected).subList(1, expected.length), Longs.asList(if3)
                                                                              .subList(1,
                                                                                       if3.length));

        BucketStats bs = new BucketStats(if3, 3, 3);
        assertEquals(0, bs.getXYUnderflow());
        assertEquals(1, bs.getYUnderflow(0));
        assertEquals(25, bs.getXOverflowYUnderflow());
        assertEquals(0, bs.getXUnderflow(0));
        assertEquals(0, bs.get(0, 0));
        assertEquals(0, bs.getXOverflow(0));
        assertEquals(0, bs.getXUnderflowYOverflow());
        assertEquals(0, bs.getYOverflow(0));
        assertEquals(181, bs.getXYOverflow());
    }

    private static MockFlamdexReader new2DMetricRegroupTestReader() {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2", "if3"), Arrays.asList("sf1"),
                                      Arrays.asList("if1", "if2", "if3"), 10);

        r.addIntTerm("if1", 0, 0);
        r.addIntTerm("if1", 1, 1);
        r.addIntTerm("if1", 2, 2);
        r.addIntTerm("if1", 3, 3);
        r.addIntTerm("if1", 4, 4);
        r.addIntTerm("if1", 5, 5);
        r.addIntTerm("if1", 6, 6);
        r.addIntTerm("if1", 7, 7);
        r.addIntTerm("if1", 8, 8);
        r.addIntTerm("if1", 9, 9);

        r.addIntTerm("if2", 2, 0, 1);
        r.addIntTerm("if2", 4, 2, 3);
        r.addIntTerm("if2", 6, 4, 5);
        r.addIntTerm("if2", 8, 6, 7);
        r.addIntTerm("if2", 10, 8, 9);

        r.addIntTerm("if3", 1, 0);
        r.addIntTerm("if3", 4, 1);
        r.addIntTerm("if3", 9, 2);
        r.addIntTerm("if3", 16, 3);
        r.addIntTerm("if3", 25, 4);
        r.addIntTerm("if3", 36, 5);
        r.addIntTerm("if3", 49, 6);
        r.addIntTerm("if3", 64, 7);
        r.addIntTerm("if3", 81, 8);
        r.addIntTerm("if3", 100, 9);

        r.addStringTerm("sf1", "☃", Arrays.asList(1, 4, 5, 6, 7));

        return r;
    }

    @Test
    public void testOrRegroup() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.stringOrRegroup("sf4",
                                new String[] { "asdf", "cdef" },
                                (char) 1,
                                (char) 0,
                                (char) 1);
        session.pushStat("count()");
        long[] stats = session.getGroupStats(0);
        assertEquals(6, stats[1]);
        session.close();
    }

    @Test
    public void testStuff() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("count()");
        session.regroup(new GroupRemapRule[] { new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                          true,
                                                                                          9999,
                                                                                          null,
                                                                                          false),
                                                                  1, 2) });
        session.regroup(new GroupRemapRule[] {
                                              new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                         true, 19,
                                                                                         null,
                                                                                         false), 1,
                                                                 2),
                                              new GroupRemapRule(
                                                                 2,
                                                                 new RegroupCondition("sf2", false,
                                                                                      0, "b", false),
                                                                 3, 4) });
        long[] stats = session.getGroupStats(0);
        assertEquals(10, stats[1]);
        assertEquals(5, stats[2]);
        assertEquals(4, stats[3]);
        assertEquals(1, stats[4]);
        session.close();
    }

    @Test
    public void testDynamicMetric() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.createDynamicMetric("foo");
        session.pushStat("dynamic foo");

        assertEquals(Longs.asList(0, 0), Longs.asList(session.getGroupStats(0)));

        session.updateDynamicMetric("foo", new int[] { 0, 1 });
        assertEquals(Longs.asList(0, 20), Longs.asList(session.getGroupStats(0)));

        session.regroup(new GroupRemapRule[] { new GroupRemapRule(1, new RegroupCondition("if2",
                                                                                          true, 0,
                                                                                          null,
                                                                                          false),
                                                                  1, 2) });
        assertEquals(Longs.asList(0, 15, 5), Longs.asList(session.getGroupStats(0)));

        session.updateDynamicMetric("foo", new int[] { 0, 0, -2 });
        assertEquals(Longs.asList(0, 15, -5), Longs.asList(session.getGroupStats(0)));

        // reset all to group 1
        session.regroup(new GroupRemapRule[] {
                                              new GroupRemapRule(1, new RegroupCondition("if2",
                                                                                         true, 0,
                                                                                         null,
                                                                                         false), 1,
                                                                 1),
                                              new GroupRemapRule(2, new RegroupCondition("if2",
                                                                                         true, 0,
                                                                                         null,
                                                                                         false), 1,
                                                                 1) });
        assertEquals(Longs.asList(0, 10), Longs.asList(session.getGroupStats(0)).subList(0, 2));
    }

    @Test
    public void testRandomMultiRegroup_firstIndexLessThan() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);

        // normal case -- 0.5 falls in the [0.4, 0.7) bucket, which is the
        // fourth (index == 3) in the list of:
        // [0.0, 0.1)
        // [0.1, 0.3)
        // [0.3, 0.4)
        // [0.4, 0.7)
        // [0.7, 0.9)
        // [0.9, 1.0]
        assertEquals(3, session.indexOfFirstLessThan(0.5, new double[] { 0.1, 0.3, 0.4, 0.7, 0.9 }));

        // less than all
        assertEquals(0, session.indexOfFirstLessThan(0.0, new double[] { 0.1, 0.2, 0.3 }));

        // empty array
        assertEquals(0, session.indexOfFirstLessThan(0.5, new double[] {}));

        // less than last element
        assertEquals(3, session.indexOfFirstLessThan(0.8, new double[] { 0.1, 0.4, 0.5, 0.9 }));

        // greater than all
        assertEquals(4, session.indexOfFirstLessThan(0.95, new double[] { 0.1, 0.4, 0.5, 0.9 }));

        // equal to last (at index 4)
        assertEquals(4, session.indexOfFirstLessThan(0.9, new double[] { 0.1, 0.4, 0.5, 0.9 }));
    }

    @Test
    public void testRandomMultiRegroup_ensureValidMultiRegroupArrays() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);

        // ******************************** Stuff that's OK:
        // normal case
        session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.5, 0.7, 0.9 },
                                              new int[] { 1, 2, 3, 4, 5 });

        // ******************************** Stuff that's not OK:
        // Bad lengths
        try {
            session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.2, 0.3 }, new int[] { 1, 2,
                                                                                             3 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected
        try {
            session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.5, 0.7 }, new int[] { 1, 2,
                                                                                             3, 4,
                                                                                             5, 6 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected
        try {
            session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.3, 0.5, 0.7, 0.9 },
                                                  new int[] { 1, 2, 3 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected

        // Percentages not in order
        try {
            session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.5, 0.3 }, new int[] { 1, 2,
                                                                                             3, 4 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected

        // Percentages out of bounds
        try {
            session.ensureValidMultiRegroupArrays(new double[] { -0.001, 0.1, 0.5, 0.7 },
                                                  new int[] { 1, 2, 3, 4, 5 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected
        try {
            session.ensureValidMultiRegroupArrays(new double[] { 0.1, 0.5, 0.7, 1.00001 },
                                                  new int[] { 1, 2, 3, 4, 5 });
            fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        } // expected
    }

    @Test
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);

        // Expected
        // ( @see MakeAFlamdex.make() )
        final String regroupField = "sf1";
        final ImhotepChooser chooser = new ImhotepChooser("salt", -1.0);
        final HashSet<Integer> noTerm =
                new HashSet<Integer>(Arrays.asList(2, 4, 7, 10, 11, 12, 13, 14, 15, 17, 18));

        final double[] percentages = new double[] { 0.10, 0.50 };
        final int[] resultGroups = new int[] { 5, 6, 7 };

        String[] docIdToTerm = new String[] { "", // 0
                                             "a", // 1
                                             null, // 2
                                             "hello world", // 3
                                             null, // 4
                                             "", // 5
                                             "a", // 6
                                             null, // 7
                                             "a", // 8
                                             "hello world", // 9
                                             null, // 10
                                             null, // 11
                                             null, // 12
                                             null, // 13
                                             null, // 14
                                             null, // 15
                                             "hello world", // 16
                                             null, // 17
                                             null, // 18
                                             "a" // 19
        };
        final Map<String, Integer> termToGroup = Maps.newHashMap();
        for (String term : Arrays.asList("", "a", "hello world")) {
            double hashValue = chooser.getValue(term);
            if (hashValue < 0.10) {
                termToGroup.put(term, 5);
            } else if (hashValue < 0.50) {
                termToGroup.put(term, 6);
            } else {
                termToGroup.put(term, 7);
            }
        }
        termToGroup.put(null, 1);

        // Actual
        session.randomMultiRegroup(regroupField, false, "salt", 1, percentages, resultGroups);

        // Make sure they're in the correct groups
        int[] docIdToGroup = new int[20];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docId = 0; docId < 20; docId++) {
            final int actualGroup = docIdToGroup[docId];
            final int expectedGroup = termToGroup.get(docIdToTerm[docId]);
            assertEquals("doc id #" + docId + " was misgrouped", expectedGroup, actualGroup);
        }
    }

    @Test
    public void testSingleMultisplitIntRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 11);
        for (int i = 1; i <= 10; i++) {
            List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
            }
            r.addIntTerm("if1", i, l);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        final RegroupCondition[] conditions = new RegroupCondition[10];
        final int[] positiveGroups = new int[10];
        for (int i = 1; i <= 10; i++) {
            conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
            positiveGroups[i - 1] = i;
        }
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(1, 0, positiveGroups,
                                                                            conditions) });
        int[] docIdToGroup = new int[11];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docId = 0; docId < 10; docId++) {
            final int actualGroup = docIdToGroup[docId];
            final int expectedGroup = docId + 1;
            assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
        }
        assertEquals("doc id #10 should be in no group", 0, docIdToGroup[10]);
    }

    @Test
    public void testSingleMultisplitStringRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.<String> asList(), Arrays.asList("sf1"),
                                      Arrays.<String> asList(), 11);
        for (int i = 1; i <= 10; i++) {
            List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
            }
            r.addStringTerm("sf1", "" + i, l);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        final RegroupCondition[] conditions = new RegroupCondition[10];
        final int[] positiveGroups = new int[10];
        for (int i = 1; i <= 10; i++) {
            conditions[i - 1] = new RegroupCondition("sf1", false, 0, "" + i, false);
            positiveGroups[i - 1] = i;
        }
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(1, 0, positiveGroups,
                                                                            conditions) });
        int[] docIdToGroup = new int[11];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docId = 0; docId < 10; docId++) {
            final int actualGroup = docIdToGroup[docId];
            final int expectedGroup = docId + 1;
            assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
        }
        assertEquals("doc id #10 should be in no group", 0, docIdToGroup[10]);
    }

    @Test
    public void testParallelMultisplitIntRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 22);
        for (int i = 1; i <= 10; i++) {
            List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
                l.add(10 + (j - 1));
            }
            r.addIntTerm("if1", i, l);
        }

        // Add 0-9 to if2 so we can split it out
        List<Integer> l = Lists.newArrayList();
        for (int i = 0; i < 11; i++) {
            l.add(10 + i);
        }
        r.addIntTerm("if2", 0, l);

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, null)),
                                           1, 2));
        final int[] positiveGroups = new int[10];
        final RegroupCondition[] conditions = new RegroupCondition[10];
        for (int i = 1; i <= 10; i++) {
            positiveGroups[i - 1] = i;
            conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
        }
        session.regroup(new GroupMultiRemapRule[] {
                                                   new GroupMultiRemapRule(1, 0, positiveGroups,
                                                                           conditions),
                                                   new GroupMultiRemapRule(2, 0, positiveGroups,
                                                                           conditions) });
        int[] docIdToGroup = new int[22];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docId = 0; docId < 10; docId++) {
            final int actualGroup = docIdToGroup[docId];
            final int expectedGroup = docId + 1;
            assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
            assertEquals("doc id #" + (10 + docId) + " was misgrouped;", expectedGroup, actualGroup);
        }
        assertEquals("doc id #20 should be in no group", 0, docIdToGroup[20]);
        assertEquals("doc id #21 should be in no group", 0, docIdToGroup[21]);
    }

    @Test
    public void testParallelMultisplitStringRegroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.<String> asList(), Arrays.asList("sf1", "sf2"),
                                      Arrays.<String> asList(), 22);
        for (int i = 1; i <= 10; i++) {
            List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
                l.add(10 + (j - 1));
            }
            r.addStringTerm("sf1", "" + i, l);
        }

        // Add 0-9 to if2 so we can split it out
        List<Integer> l = Lists.newArrayList();
        for (int i = 0; i < 11; i++) {
            l.add(10 + i);
        }
        r.addStringTerm("sf2", "0", l);

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new QueryRemapRule(1, Query.newTermQuery(new Term("sf2", false, 0, "0")),
                                           1, 2));
        final int[] positiveGroups = new int[10];
        final RegroupCondition[] conditions = new RegroupCondition[10];
        for (int i = 1; i <= 10; i++) {
            positiveGroups[i - 1] = i;
            conditions[i - 1] = new RegroupCondition("sf1", false, 0, "" + i, false);
        }
        session.regroup(new GroupMultiRemapRule[] {
                                                   new GroupMultiRemapRule(1, 0, positiveGroups,
                                                                           conditions),
                                                   new GroupMultiRemapRule(2, 0, positiveGroups,
                                                                           conditions) });
        int[] docIdToGroup = new int[22];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docId = 0; docId < 10; docId++) {
            final int actualGroup = docIdToGroup[docId];
            final int expectedGroup = docId + 1;
            assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
            assertEquals("doc id #" + (10 + docId) + " was misgrouped;", expectedGroup, actualGroup);
        }
        assertEquals("doc id #20 should be in no group", 0, docIdToGroup[20]);
        assertEquals("doc id #21 should be in no group", 0, docIdToGroup[21]);
    }

    @Test
    public void testMultisplitTargetingNonexistentGroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 11);
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1000,
                                                                            1234,
                                                                            new int[] { 1 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          "if1",
                                                                                                                          true,
                                                                                                                          1,
                                                                                                                          null,
                                                                                                                          false) }) });
    }

    @Test
    public void testIntMultiInequalitySplit() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 10);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
            r.addIntTerm("if2", i, i);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            5,
                                                                            new int[] { 1, 2, 3 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         5,
                                                                                                                         null,
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "if2",
                                                                                                                         true,
                                                                                                                         7,
                                                                                                                         null,
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         9,
                                                                                                                         null,
                                                                                                                         true),
                                                                            // new
                                                                            // RegroupCondition("if2",true,4,null,true),
                                                                            }) });
        final int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < 10; i++) {
            if (i <= 5) {
                assertEquals(1, docIdToGroup[i]);
            } else if (i <= 7) {
                assertEquals(2, docIdToGroup[i]);
            } else if (i <= 9) {
                assertEquals(3, docIdToGroup[i]);
            } else {
                assertEquals(5, docIdToGroup[i]);
            }
        }
    }

    @Test
    public void testMultisplitGeneralInputValidation() throws ImhotepOutOfMemoryException {
        // count mismatch #1
        {
            MockFlamdexReader r =
                    new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                          Arrays.<String> asList(), 10);
            ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
            try {
                session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                    1,
                                                                                    0,
                                                                                    new int[] { 1 },
                                                                                    new RegroupCondition[] {
                                                                                                            new RegroupCondition(
                                                                                                                                 "if1",
                                                                                                                                 true,
                                                                                                                                 1,
                                                                                                                                 null,
                                                                                                                                 true),
                                                                                                            new RegroupCondition(
                                                                                                                                 "if1",
                                                                                                                                 true,
                                                                                                                                 1,
                                                                                                                                 null,
                                                                                                                                 true), }) });
                fail("Improperly handles having more conditions than positive groups");
            } catch (IllegalArgumentException e) {
            }
        }

        // count mismatch #2
        {
            MockFlamdexReader r =
                    new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                          Arrays.<String> asList(), 10);
            ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
            try {
                session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                    1,
                                                                                    0,
                                                                                    new int[] { 1,
                                                                                               2 },
                                                                                    new RegroupCondition[] { new RegroupCondition(
                                                                                                                                  "if1",
                                                                                                                                  true,
                                                                                                                                  1,
                                                                                                                                  null,
                                                                                                                                  true), }) });
                fail("Improperly handles having fewer conditions than positive groups");
            } catch (IllegalArgumentException e) {
            }
        }

    }

    @Test
    public void testIntMultisplitInequalityInputValidation() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 10);
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        try {
            session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                1,
                                                                                5,
                                                                                new int[] { 1, 2 },
                                                                                new RegroupCondition[] {
                                                                                                        new RegroupCondition(
                                                                                                                             "if1",
                                                                                                                             true,
                                                                                                                             7,
                                                                                                                             null,
                                                                                                                             true),
                                                                                                        new RegroupCondition(
                                                                                                                             "if1",
                                                                                                                             true,
                                                                                                                             4,
                                                                                                                             null,
                                                                                                                             true), }) });
            fail("Improperly handles unreachable inequality splits");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testStringMultisplitInequalityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1");
        final List<String> emptyList = Arrays.<String> asList();
        MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10);
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        try {
            session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                1,
                                                                                5,
                                                                                new int[] { 1, 2 },
                                                                                new RegroupCondition[] {
                                                                                                        new RegroupCondition(
                                                                                                                             "sf1",
                                                                                                                             false,
                                                                                                                             0,
                                                                                                                             "7",
                                                                                                                             true),
                                                                                                        new RegroupCondition(
                                                                                                                             "sf1",
                                                                                                                             false,
                                                                                                                             0,
                                                                                                                             "4",
                                                                                                                             true), }) });
            fail("Improperly handles unreachable inequality splits");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testStringMultisplitEqualityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1");
        final List<String> emptyList = Arrays.<String> asList();
        MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10);
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        // verify doesn't fail
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            5,
                                                                            new int[] { 1, 2 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "a",
                                                                                                                         false),
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "a",
                                                                                                                         true)

                                                                            }) });
        try {
            session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                1,
                                                                                5,
                                                                                new int[] { 1, 2 },
                                                                                new RegroupCondition[] {
                                                                                                        new RegroupCondition(
                                                                                                                             "sf1",
                                                                                                                             false,
                                                                                                                             0,
                                                                                                                             "a",
                                                                                                                             false),
                                                                                                        new RegroupCondition(
                                                                                                                             "sf1",
                                                                                                                             false,
                                                                                                                             0,
                                                                                                                             "a",
                                                                                                                             false) }) });
            fail("Improperly handles unreachable equality splits");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testIntMultisplitEqualityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("if1");
        final List<String> emptyList = Arrays.<String> asList();
        MockFlamdexReader r = new MockFlamdexReader(fields, emptyList, emptyList, 10);
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        // verify doesn't fail
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            5,
                                                                            new int[] { 1, 2 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         1,
                                                                                                                         null,
                                                                                                                         false),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         1,
                                                                                                                         null,
                                                                                                                         true)

                                                                            }) });
        try {
            session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                                1,
                                                                                5,
                                                                                new int[] { 1, 2 },
                                                                                new RegroupCondition[] {
                                                                                                        new RegroupCondition(
                                                                                                                             "if1",
                                                                                                                             true,
                                                                                                                             1,
                                                                                                                             null,
                                                                                                                             false),
                                                                                                        new RegroupCondition(
                                                                                                                             "if1",
                                                                                                                             true,
                                                                                                                             1,
                                                                                                                             null,
                                                                                                                             false) }) });
            fail("Improperly handles unreachable equality splits");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testIntMultiParallelInequalitySplit() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 20);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i, i + 10);
        }
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if2", 1, Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            1,
                                                                            new int[] { 2 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          "if2",
                                                                                                                          true,
                                                                                                                          1,
                                                                                                                          null,
                                                                                                                          false) }) });
        session.regroup(new GroupMultiRemapRule[] {
                                                   new GroupMultiRemapRule(
                                                                           1,
                                                                           5,
                                                                           new int[] { 1, 2, 3 },
                                                                           new RegroupCondition[] {
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        5,
                                                                                                                        null,
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        7,
                                                                                                                        null,
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        9,
                                                                                                                        null,
                                                                                                                        true), }),
                                                   new GroupMultiRemapRule(
                                                                           2,
                                                                           10,
                                                                           new int[] { 6, 7, 8 },
                                                                           new RegroupCondition[] {
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        5,
                                                                                                                        null,
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        7,
                                                                                                                        null,
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "if1",
                                                                                                                        true,
                                                                                                                        9,
                                                                                                                        null,
                                                                                                                        true), }) });
        final int[] docIdToGroup = new int[20];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < 10; i++) {
            if (i <= 5) {
                assertEquals(1, docIdToGroup[i]);
                assertEquals(6, docIdToGroup[i + 10]);
            } else if (i <= 7) {
                assertEquals(2, docIdToGroup[i]);
                assertEquals(7, docIdToGroup[i + 10]);
            } else if (i <= 9) {
                assertEquals(3, docIdToGroup[i]);
                assertEquals(8, docIdToGroup[i + 10]);
            } else {
                assertEquals(5, docIdToGroup[i]);
                assertEquals(10, docIdToGroup[i + 10]);
            }
        }
    }

    @Test
    public void testStringMultiInequalitySplit() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1", "sf2");
        final List<String> emptyList = Arrays.<String> asList();
        MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10);
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf1", "" + i, i);
            r.addStringTerm("sf2", "" + i, i);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            5,
                                                                            new int[] { 1, 2, 3 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "5",
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "sf2",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "7",
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "9",
                                                                                                                         true), }) });
        final int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < 10; i++) {
            if (i <= 5) {
                assertEquals(1, docIdToGroup[i]);
            } else if (i <= 7) {
                assertEquals(2, docIdToGroup[i]);
            } else if (i <= 9) {
                assertEquals(3, docIdToGroup[i]);
            } else {
                assertEquals(5, docIdToGroup[i]);
            }
        }
    }

    @Test
    public void testStringMultiParallelInequalitySplit() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1", "sf2");
        final List<String> empty = Arrays.<String> asList();
        MockFlamdexReader r = new MockFlamdexReader(empty, fields, empty, 20);
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf1", "" + i, i, i + 10);
        }
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf2", "1", Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            1,
                                                                            new int[] { 2 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          "sf2",
                                                                                                                          false,
                                                                                                                          0,
                                                                                                                          "1",
                                                                                                                          false) }) });
        session.regroup(new GroupMultiRemapRule[] {
                                                   new GroupMultiRemapRule(
                                                                           1,
                                                                           5,
                                                                           new int[] { 1, 2, 3 },
                                                                           new RegroupCondition[] {
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "5",
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "7",
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "9",
                                                                                                                        true), }),
                                                   new GroupMultiRemapRule(
                                                                           2,
                                                                           10,
                                                                           new int[] { 6, 7, 8 },
                                                                           new RegroupCondition[] {
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "5",
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "7",
                                                                                                                        true),
                                                                                                   new RegroupCondition(
                                                                                                                        "sf1",
                                                                                                                        false,
                                                                                                                        0,
                                                                                                                        "9",
                                                                                                                        true), }) });
        final int[] docIdToGroup = new int[20];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < 10; i++) {
            if (i <= 5) {
                assertEquals(1, docIdToGroup[i]);
                assertEquals(6, docIdToGroup[i + 10]);
            } else if (i <= 7) {
                assertEquals(2, docIdToGroup[i]);
                assertEquals(7, docIdToGroup[i + 10]);
            } else if (i <= 9) {
                assertEquals(3, docIdToGroup[i]);
                assertEquals(8, docIdToGroup[i + 10]);
            } else {
                assertEquals(5, docIdToGroup[i]);
                assertEquals(10, docIdToGroup[i + 10]);
            }
        }
    }

    @Test
    public void testManyGroupMultiRemapRuleThings() throws ImhotepOutOfMemoryException {
        final List<String> intFields = Arrays.asList("if1", "if2");
        final List<String> stringFields = Arrays.asList("sf1", "sf2");
        final List<String> emptyList = Arrays.<String> asList();
        final int numDocs = 7;
        MockFlamdexReader r = new MockFlamdexReader(intFields, stringFields, emptyList, numDocs);
        int[] i1terms = new int[] { 1, 2, 3, 4, 5, 6 };
        int[] i2terms = new int[] { 5, 1, 2, 3, 8, 7 };
        String[] s1terms = new String[] { "e", "d", "c", "bc", "b", "a" };
        String[] s2terms = new String[] { "foo", "bar", "baz", "foo", "bar", "baz" };
        addIntField(r, "if1", i1terms);
        addIntField(r, "if2", i2terms);
        addStringField(r, "sf1", s1terms);
        addStringField(r, "sf2", s2terms);

        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        testAllInequalitySplits(numDocs, "if1", i1terms, session);
        testAllInequalitySplits(numDocs, "if2", i2terms, session);
        testAllInequalitySplits(numDocs, "sf1", s1terms, session);
        testAllInequalitySplits(numDocs, "sf2", s2terms, session);

        final int[] docIdToGroup = new int[numDocs];

        // Try parallel inequality regroups, verify that later ones do not
        // override earlier ones.
        session.resetGroups();
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            2,
                                                                            new int[] { 1, 3 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         3,
                                                                                                                         null,
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "bc",
                                                                                                                         true) }) });
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < i1terms.length; i++) {
            if (i1terms[i] <= 3) {
                assertEquals(1, docIdToGroup[i]);
            } else {
                assertEquals(3, docIdToGroup[i]);
            }
        }
        for (int i = i1terms.length; i < numDocs; i++) {
            assertEquals(2, docIdToGroup[i]);
        }

        // Try the opposite ordering of priority
        session.resetGroups();
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            2,
                                                                            new int[] { 1, 3 },
                                                                            new RegroupCondition[] {
                                                                                                    new RegroupCondition(
                                                                                                                         "sf1",
                                                                                                                         false,
                                                                                                                         0,
                                                                                                                         "bc",
                                                                                                                         true),
                                                                                                    new RegroupCondition(
                                                                                                                         "if1",
                                                                                                                         true,
                                                                                                                         3,
                                                                                                                         null,
                                                                                                                         true) }) });
        session.exportDocIdToGroupId(docIdToGroup);
        for (int i = 0; i < s1terms.length; i++) {
            if (s1terms[i].compareTo("bc") <= 0) {
                assertEquals(1, docIdToGroup[i]);
            } else {
                assertEquals(3, docIdToGroup[i]);
            }
        }
        for (int i = s1terms.length; i < numDocs; i++) {
            assertEquals(2, docIdToGroup[i]);
        }
    }

    @Test
    public void testEmptyMultisplit() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 10);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            2,
                                                                            new int[] { 1 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          "if1",
                                                                                                                          true,
                                                                                                                          5,
                                                                                                                          null,
                                                                                                                          true) }) });
        session.regroup(new GroupMultiRemapRule[] {});
        final int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int group : docIdToGroup) {
            assertEquals(0, group);
        }
        session.close();
    }

    @Test
    public void testUntargetedGroup() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1"), Arrays.<String> asList(),
                                      Arrays.<String> asList(), 10);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
        }
        ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            2,
                                                                            3,
                                                                            new int[] { 1 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          "if1",
                                                                                                                          true,
                                                                                                                          1,
                                                                                                                          null,
                                                                                                                          false) }) });
        final int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int group : docIdToGroup) {
            assertEquals(0, group);
        }
    }

    private void testAllInequalitySplits(int numDocs,
                                         String field,
                                         int[] terms,
                                         ImhotepLocalSession session) throws ImhotepOutOfMemoryException {
        testTermInequalitySplit(numDocs, field, terms, session, Integer.MIN_VALUE);
        testTermInequalitySplit(numDocs, field, terms, session, Integer.MAX_VALUE);
        for (int term : terms) {
            testTermInequalitySplit(numDocs, field, terms, session, term);
            testTermInequalitySplit(numDocs, field, terms, session, term - 1);
            testTermInequalitySplit(numDocs, field, terms, session, term + 1);
        }
    }

    private void testTermInequalitySplit(int numDocs,
                                         String field,
                                         int[] terms,
                                         ImhotepLocalSession session,
                                         int term) throws ImhotepOutOfMemoryException {
        session.resetGroups();
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            0,
                                                                            new int[] { 1 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          field,
                                                                                                                          true,
                                                                                                                          term,
                                                                                                                          null,
                                                                                                                          true) }) });
        final int[] docIdToGroup = new int[numDocs];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docid = 0; docid < terms.length; docid++) {
            if (terms[docid] <= term) {
                assertEquals(1, docIdToGroup[docid]);
            } else {
                assertEquals(0, docIdToGroup[docid]);
            }
        }
        for (int docid = terms.length; docid < numDocs; docid++) {
            assertEquals(0, docIdToGroup[docid]);
        }
    }

    private void testAllInequalitySplits(int numDocs,
                                         String field,
                                         String[] terms,
                                         ImhotepLocalSession session) throws ImhotepOutOfMemoryException {
        testTermInequalitySplit(numDocs, field, terms, session, "");
        for (String term : terms) {
            if (term.length() >= 1) {
                testTermInequalitySplit(numDocs,
                                        field,
                                        terms,
                                        session,
                                        term.substring(0, term.length() - 1));
            }
            testTermInequalitySplit(numDocs, field, terms, session, term);
            testTermInequalitySplit(numDocs, field, terms, session, term + "a");
        }
    }

    private void testTermInequalitySplit(int numDocs,
                                         String field,
                                         String[] terms,
                                         ImhotepLocalSession session,
                                         String term) throws ImhotepOutOfMemoryException {
        session.resetGroups();
        session.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(
                                                                            1,
                                                                            0,
                                                                            new int[] { 1 },
                                                                            new RegroupCondition[] { new RegroupCondition(
                                                                                                                          field,
                                                                                                                          false,
                                                                                                                          0,
                                                                                                                          term,
                                                                                                                          true) }) });
        final int[] docIdToGroup = new int[numDocs];
        session.exportDocIdToGroupId(docIdToGroup);
        for (int docid = 0; docid < terms.length; docid++) {
            if (terms[docid].compareTo(term) <= 0) {
                assertEquals(1, docIdToGroup[docid]);
            } else {
                assertEquals(0, docIdToGroup[docid]);
            }
        }
        for (int docid = terms.length; docid < numDocs; docid++) {
            assertEquals(0, docIdToGroup[docid]);
        }
    }

    private void addIntField(MockFlamdexReader r, String fieldName, int[] terms) {
        Map<Integer, List<Integer>> map = Maps.newHashMap();
        for (int ix = 0; ix < terms.length; ix++) {
            if (!map.containsKey(terms[ix])) {
                map.put(terms[ix], Lists.<Integer> newArrayList());
            }
            map.get(terms[ix]).add(ix);
        }
        for (final Integer term : map.keySet()) {
            final List<Integer> docs = map.get(term);
            r.addIntTerm(fieldName, term, docs);
        }
    }

    private void addStringField(MockFlamdexReader r, String fieldName, String[] terms) {
        Map<String, List<Integer>> map = Maps.newHashMap();
        for (int ix = 0; ix < terms.length; ix++) {
            if (!map.containsKey(terms[ix])) {
                map.put(terms[ix], Lists.<Integer> newArrayList());
            }
            map.get(terms[ix]).add(ix);
        }
        for (final String term : map.keySet()) {
            final List<Integer> docs = map.get(term);
            r.addStringTerm(fieldName, term, docs);
        }
    }

    @Test
    public void testConditionalUpdateDynamicMetric() throws ImhotepOutOfMemoryException {
        final int[] iCanCount = new int[10];
        for (int i = 0; i < iCanCount.length; i++) {
            iCanCount[i] = i;
        }
        final MockFlamdexReader r = new MockFlamdexReader();
        r.addIntTerm("if1", 0, 0, 2, 4, 6, 8);
        r.addIntTerm("if1", 1, 1, 3, 5, 7, 9);
        r.addStringTerm("sf1", "even", 0, 2, 4, 6, 8);
        r.addStringTerm("sf1", "odd", 1, 3, 5, 7, 9);
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        final String METRIC_NAME = "test metric!";
        session.createDynamicMetric(METRIC_NAME);
        final long[] exported = new long[10];
        // Should be a no-op
        session.conditionalUpdateDynamicMetric(METRIC_NAME,
                                               new RegroupCondition[] {
                                                                       new RegroupCondition("if1",
                                                                                            true,
                                                                                            0,
                                                                                            null,
                                                                                            false),
                                                                       new RegroupCondition("sf1",
                                                                                            false,
                                                                                            0,
                                                                                            "even",
                                                                                            false) },
                                               new int[] { 100, -100 });
        final DynamicMetric metric = session.getDynamicMetrics().get(METRIC_NAME);
        metric.lookup(iCanCount, exported, 10);
        Assert.assertArrayEquals(new long[10], exported);
        // Should increase odd terms by 10
        session.conditionalUpdateDynamicMetric(METRIC_NAME,
                                               new RegroupCondition[] { new RegroupCondition("if1",
                                                                                             true,
                                                                                             1,
                                                                                             null,
                                                                                             false) },
                                               new int[] { 10 });
        metric.lookup(iCanCount, exported, 10);
        Assert.assertArrayEquals(new long[] { 0, 10, 0, 10, 0, 10, 0, 10, 0, 10 }, exported);
    }

    @Test
    public void testPushStatFloatScale() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session = new ImhotepJavaLocalSession(r);
        session.pushStat("floatscale floatfield*100+9000"); // like iplat
        long[] stats = session.getGroupStats(0);
        long scaledSum = stats[1];
        // we have 5 documents for each of 4 values: 1.5, 2.5, 0 and 18000
        long expectedSum =
                (long) ((1.5 * 100 + 9000) + (2.5 * 100 + 9000) + (0 * 100 + 9000) + (18000 * 100 + 9000)) * 5;
        Assert.assertEquals("Sum of scaled values", expectedSum, scaledSum, 0.001);
        session.close();
    }

    @Test
    public void testGroup0Filtering() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final FlamdexReader r1 = MakeAFlamdex.make();
        final ImhotepLocalSession session1 =
            new ImhotepJavaLocalSession(r1, "/tmp/imhotep.test",
                                        new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                                        null);
        session1.pushStat("count()");
        session1.createDynamicMetric("foo");
        session1.createDynamicMetric("bar");
        int[] bar = { 0, 13 };
        session1.updateDynamicMetric("bar", bar);
        session1.regroup(new GroupRemapRule[] { new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                           true,
                                                                                           9999,
                                                                                           null,
                                                                                           false),
                                                                   1, 2) });
        int[] foo = { 0, 1, 2 };
        session1.updateDynamicMetric("foo", foo);
        session1.regroup(new GroupRemapRule[] {
                                               new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                          true, 19,
                                                                                          null,
                                                                                          false),
                                                                  0, 2),
                                               new GroupRemapRule(2, new RegroupCondition("sf2",
                                                                                          false, 0,
                                                                                          "b",
                                                                                          false),
                                                                  3, 4) });
        int[] fo = { 0, 0, 0, 0, 1 };
        session1.updateDynamicMetric("foo", fo);

        long[] stats1 = session1.getGroupStats(0);
        assertEquals(10, stats1[0]);
        assertEquals(0, stats1[1]);
        assertEquals(5, stats1[2]);
        assertEquals(4, stats1[3]);
        assertEquals(1, stats1[4]);

        /* optimize session */
        session1.rebuildAndFilterIndexes(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"));

        GroupLookup gl = session1.docIdToGroup;
        assertEquals(5, gl.getNumGroups());
        assertEquals(10, gl.size());
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " in wrong group", 2, gl.get(i));
            }
            if (i >= 5 && i < 7) {
                assertEquals(Integer.toString(i) + " in wrong group", 3, gl.get(i));
            }
            if (i >= 7 && i < 8) {
                assertEquals(Integer.toString(i) + " in wrong group", 4, gl.get(i));
            }
            if (i >= 8 && i < 10) {
                assertEquals(Integer.toString(i) + " in wrong group", 3, gl.get(i));
            }
        }

        /* check the dynamic metric */
        Map<String, DynamicMetric> dynamicMetrics = session1.getDynamicMetrics();
        /* check all the groups are there */
        assertEquals(dynamicMetrics.size(), 2);
        assertNotNull(dynamicMetrics.get("foo"));
        assertNotNull(dynamicMetrics.get("bar"));

        /* check dynamic metrics per group */
        DynamicMetric dm = dynamicMetrics.get("foo");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             1,
                             dm.lookupSingleVal(i));
            }
            if (i >= 5 && i < 7) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             2,
                             dm.lookupSingleVal(i));
            }
            if (i >= 7 && i < 8) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             3,
                             dm.lookupSingleVal(i));
            }
            if (i >= 8 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             2,
                             dm.lookupSingleVal(i));
            }
        }

        dm = dynamicMetrics.get("bar");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             13,
                             dm.lookupSingleVal(i));
            }
        }

        /* try another regroup */
        session1.createDynamicMetric("cat");
        int[] cat = { 0, 17 };
        session1.updateDynamicMetric("cat", cat);
        session1.regroup(new GroupRemapRule[] {
                                               new GroupRemapRule(2, new RegroupCondition("if3",
                                                                                          true, 5,
                                                                                          null,
                                                                                          false),
                                                                  1, 6),
                                               new GroupRemapRule(3, new RegroupCondition("if3",
                                                                                          true,
                                                                                          10000,
                                                                                          null,
                                                                                          false),
                                                                  2, 7) });
        stats1 = session1.getGroupStats(0);
        assertEquals(1, stats1[0]);
        assertEquals(5, stats1[1]);
        assertEquals(4, stats1[2]);

        int[] foo2 = { 0, 7, 11 };
        session1.updateDynamicMetric("foo", foo2);

        /* optimize session */
        session1.rebuildAndFilterIndexes(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"));

        stats1 = session1.getGroupStats(0);
        assertEquals(0, stats1[0]);
        assertEquals(5, stats1[1]);
        assertEquals(4, stats1[2]);

        gl = session1.docIdToGroup;
        assertEquals(3, gl.getNumGroups());
        assertEquals(9, gl.size());
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " in wrong group", 1, gl.get(i));
            }
            if (i >= 5 && i < 9) {
                assertEquals(Integer.toString(i) + " in wrong group", 2, gl.get(i));
            }
        }

        /* check dynamic metrics per group */
        dynamicMetrics = session1.getDynamicMetrics();
        dm = dynamicMetrics.get("foo");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             8,
                             dm.lookupSingleVal(i));
            }
            if (i >= 5 && i < 9) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             13,
                             dm.lookupSingleVal(i));
            }
        }

        dm = dynamicMetrics.get("bar");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 9) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             13,
                             dm.lookupSingleVal(i));
            }
        }

        session1.close();
    }

    @Test
    public void testOptimizeThenReset() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final FlamdexReader r1 = MakeAFlamdex.make();
        final ImhotepLocalSession session1 =
                new ImhotepJavaLocalSession(r1,
                                        "/tmp/imhotep.test",
                                        new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                                        null);
        session1.pushStat("count()");
        session1.createDynamicMetric("foo");
        session1.createDynamicMetric("bar");
        int[] bar = { 0, 13 };
        session1.updateDynamicMetric("bar", bar);
        session1.regroup(new GroupRemapRule[] { new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                           true,
                                                                                           9999,
                                                                                           null,
                                                                                           false),
                                                                   1, 2) });
        int[] foo = { 0, 1, 2 };
        session1.updateDynamicMetric("foo", foo);
        session1.regroup(new GroupRemapRule[] {
                                               new GroupRemapRule(1, new RegroupCondition("if3",
                                                                                          true, 19,
                                                                                          null,
                                                                                          false),
                                                                  0, 2),
                                               new GroupRemapRule(2, new RegroupCondition("sf2",
                                                                                          false, 0,
                                                                                          "b",
                                                                                          false),
                                                                  3, 4) });
        int[] fo = { 0, 0, 0, 0, 1 };
        session1.updateDynamicMetric("foo", fo);

        /* optimize session */
        session1.rebuildAndFilterIndexes(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"));

        /* try another regroup */
        session1.createDynamicMetric("cat");
        int[] cat = { 0, 3, 3, 3, 3 };
        session1.updateDynamicMetric("cat", cat);
        int[] fo2 = { 0, 0, 0, 0, 2 };
        session1.updateDynamicMetric("foo", fo2);
        session1.regroup(new GroupRemapRule[] {
                                               new GroupRemapRule(2, new RegroupCondition("if3",
                                                                                          true, 5,
                                                                                          null,
                                                                                          false),
                                                                  1, 6),
                                               new GroupRemapRule(3, new RegroupCondition("if3",
                                                                                          true,
                                                                                          10000,
                                                                                          null,
                                                                                          false),
                                                                  2, 7) });

        int[] foo2 = { 0, 7, 11 };
        session1.updateDynamicMetric("foo", foo2);

        /* optimize session */
        session1.rebuildAndFilterIndexes(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"));

        int[] foo3 = { 0, 0, 1 };
        session1.updateDynamicMetric("foo", foo3);

        /* reset */
        session1.resetGroups();

        /* check groups */
        GroupLookup gl = session1.docIdToGroup;
        assertEquals(2, gl.getNumGroups());
        assertEquals(20, gl.size());

        /* check the dynamic metric */
        Map<String, DynamicMetric> dynamicMetrics = session1.getDynamicMetrics();
        /* check all the groups are there */
        assertEquals(dynamicMetrics.size(), 3);
        assertNotNull(dynamicMetrics.get("foo"));
        assertNotNull(dynamicMetrics.get("bar"));
        assertNotNull(dynamicMetrics.get("cat"));

        /* check dynamic metrics per group */
        DynamicMetric dm = dynamicMetrics.get("bar");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             13,
                             dm.lookupSingleVal(i));
            }
        }

        dm = dynamicMetrics.get("cat");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             0,
                             dm.lookupSingleVal(i));
            }
            if (i >= 5 && i < 15) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             3,
                             dm.lookupSingleVal(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             0,
                             dm.lookupSingleVal(i));
            }
        }

        dm = dynamicMetrics.get("foo");
        for (int i = 0; i < gl.size(); i++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             1,
                             dm.lookupSingleVal(i));
            }
            if (i >= 5 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             8,
                             dm.lookupSingleVal(i));
            }
            if (i >= 10 && i < 12) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             14,
                             dm.lookupSingleVal(i));
            }
            if (i >= 12 && i < 13) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             5,
                             dm.lookupSingleVal(i));
            }
            if (i >= 13 && i < 15) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             14,
                             dm.lookupSingleVal(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric",
                             1,
                             dm.lookupSingleVal(i));
            }
        }

        session1.close();
    }

    @Test
    public void testRegexMetric() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        final ImhotepLocalSession session =
                new ImhotepLocalSession(r,
                        "/tmp/imhotep.test",
                        new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                        false, null);

        session.pushStat("regex if1:9000");
        Assert.assertArrayEquals(new long[]{0, 3}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex if3:.*9");
        Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex if3:notaninteger");
        Assert.assertArrayEquals(new long[]{0, 0}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex sf1:");
        Assert.assertArrayEquals(new long[]{0, 2}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex sf2:b");
        Assert.assertArrayEquals(new long[]{0, 4}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex floatfield:[0-9]*\\.[0-9]*");
        Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(0));
        session.popStat();

        session.pushStat("regex nonexistent:anything");
        Assert.assertArrayEquals(new long[]{0, 0}, session.getGroupStats(0));
        session.popStat();
    }
}
