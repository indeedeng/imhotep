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
 package com.indeed.imhotep.local;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.exceptions.MultiValuedFieldUidTimestampException;
import com.indeed.imhotep.group.IterativeHasherUtils;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.protobuf.Operator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestImhotepLocalSession {
    @Test
    public void testPushPopGetDepth() throws ImhotepOutOfMemoryException {
        // This test doesn't really specifically need the 2d test setup,
        // but that setup is good enoguh for this too.
        final FlamdexReader r = new2DMetricRegroupTestReader();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
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
    }

    @Test
    public void testMoreConditionsThanTargetGroups() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 16, shardDir);
        r.addIntTerm("if1", 1, 1, 3, 5, 7, 9, 11, 13, 15); // 0th bit
        r.addIntTerm("if1", 2, 2, 3, 6, 7, 10, 11, 14, 15); // 1st bit
        r.addIntTerm("if1", 4, 4, 5, 6, 7, 12, 13, 14, 15); // 2nd bit
        r.addIntTerm("if1", 8, 8, 9, 10, 11, 12, 13, 14, 15); // 2nd bit
        // 0000, 0001, 0010, 0011, 0100, 0101, 0110, 0111, 1000, 1001, ...
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final GroupMultiRemapRule gmrr;
            gmrr = new GroupMultiRemapRule(1,
                    0,
                    new int[]{1, 1, 1, 1},
                    new RegroupCondition[]{
                            new RegroupCondition("if1", true, 1, null, false),
                            new RegroupCondition("if1", true, 2, null, false),
                            new RegroupCondition("if1", true, 4, null, false),
                            new RegroupCondition("if1", true, 8, null, false),});
            session.regroup(new GroupMultiRemapRule[]{gmrr});
            final int[] arr = new int[16];
            session.exportDocIdToGroupId(arr);
            final int[] expected = new int[]{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
            assertArrayEquals(expected, arr);
        }
    }

    @Test
    public void testResetThenRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String> emptyList(),
                                      singletonList("if1"), 10, shardDir);
        r.addIntTerm("if1", 0, 1, 3, 5, 7, 9);
        r.addIntTerm("if1", 5, 0, 2, 4, 6, 8);

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    0,
                    new int[]{1},
                    new RegroupCondition[]{new RegroupCondition("if1", true, 0, "", false)}
            )});
            session.resetGroups();
            final int numGroups =
                    session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                            99999,
                            1,
                            new int[]{1},
                            new RegroupCondition[]{
                                    new RegroupCondition("if1", true, 1, "", false)
                            }
                    )});
            assertEquals(1, numGroups);
        }
    }

    @Test
    public void testTargetedMetricFilter() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = newMetricRegroupTestReader();

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final List<String> if1Stat = singletonList("if1");
            final List<String> countStat = singletonList("count()");

            Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(countStat));

            // Nothing should be 0-4, so this should do nothing.
            session.metricFilter(if1Stat, (long) 0, (long) 4, 1, 1, 0);
            Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(countStat));

            // Move the 5s to 2
            session.metricFilter(if1Stat, (long) 0, (long) 5, 1, 1, 2);
            Assert.assertArrayEquals(new long[]{0, 5, 5}, session.getGroupStats(countStat));

            // Make sure 19 and 21 do nothing to the 20
            session.metricFilter(if1Stat, (long) 19, (long) 19, 1, 1, 3);
            Assert.assertArrayEquals(new long[]{0, 5, 5}, session.getGroupStats(countStat));
            session.metricFilter(if1Stat, (long) 21, (long) 21, 1, 1, 3);
            Assert.assertArrayEquals(new long[]{0, 5, 5}, session.getGroupStats(countStat));

            // Move the 20 to 3
            session.metricFilter(if1Stat, (long) 20, (long) 20, 1, 1, 3);
            Assert.assertArrayEquals(new long[]{0, 4, 5, 1}, session.getGroupStats(countStat));
        }
    }

    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = newMetricRegroupTestReader();

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final int numGroups = session.metricRegroup(singletonList("if1"), (long) 0, (long) 20, (long) 5);
            assertEquals(7, numGroups); // 4 buckets, 2 gutters, group 0

            final int[] docIdToGroup = new int[10];
            session.exportDocIdToGroupId(docIdToGroup);
            assertEquals(Arrays.asList(2, 3, 2, 4, 2, 4, 2, 3, 2, 6), Ints.asList(docIdToGroup));
        }
    }

    @Test
    public void testMetricRegroupOneBucket() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = newMetricRegroupTestReader();

        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final List<String> if1Stat = singletonList("if1");
            // 5 buckets, no gutters, group 0
            assertTrue(testMetricRegroupOneBucket(session, if1Stat, -100, 25, 25, true, 5));
            // everything is filtered out, only group 0
            assertTrue(testMetricRegroupOneBucket(session, if1Stat, -1, 0, 1, true, 0));
            // all doc values are smaller
            assertTrue(testMetricRegroupOneBucket(session, if1Stat, 100, 105, 1, false, 6));
            // all doc values are greater
            assertTrue(testMetricRegroupOneBucket(session, if1Stat, -105, -100, 1, false, 7));
        }
    }

    private boolean testMetricRegroupOneBucket(
            final ImhotepLocalSession session,
            final List<String> testStat,
            final int min,
            final int max,
            final int intervalSize,
            final boolean noGutters,
            final int expectedGroup) throws ImhotepOutOfMemoryException {
        session.resetGroups();
        final int numGroups = session.metricRegroup(testStat, (long) min, (long) max, (long) intervalSize, noGutters);
        if (numGroups != (expectedGroup + 1)) {
            return false;
        }
        final int[] docIdToGroup = new int[10];
        session.exportDocIdToGroupId(docIdToGroup);
        for (final int group : docIdToGroup) {
            if (group != expectedGroup) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testMetricRegroup2() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = newMetricRegroupTestReader();

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            assertEquals(2,
                    session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                            1,
                            1,
                            new int[]{0},
                            new RegroupCondition[]{
                                    new RegroupCondition("sf1", false, 0, "☃", false)
                            }
                    )}));

            final int numGroups = session.metricRegroup(singletonList("if1"), (long) 9, (long) 17, (long) 4);
            assertEquals(5, numGroups); // 2 buckets, 2 gutters, group 0

            final int[] docIdToGroup = new int[10];
            session.exportDocIdToGroupId(docIdToGroup);
            assertEquals(Arrays.asList(3, 1, 3, 0, 0, 2, 3, 1, 0, 4), Ints.asList(docIdToGroup));
        }
    }

    @Test
    // Ensure that time regroups work without the unixtime field even being present in the shard when
    // the documents in the shard all map into the same group
    public void testMetricRegroupUnixtime() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r = new MockFlamdexReader(Collections.emptyList(), Collections.singletonList("testField"), Collections.emptyList(), 100);
        r.addStringTerm("testField", "term", 0, 5, 20, 53);

        final DateTime today = DateTime.now().withTimeAtStartOfDay();

        final DateTime queryStart = today.minusDays(1);
        final DateTime queryEnd = today;

        final DateTime firstShardStart = queryStart;
        final DateTime firstShardEnd = firstShardStart.plusHours(1);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, new Interval(firstShardStart, firstShardEnd))) {
            assertEquals(2, session.metricRegroup(Collections.singletonList("unixtime"), queryStart.getMillis()/1000, queryEnd.getMillis()/1000, 3600));
            assertTrue(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS) instanceof ConstantGroupLookup);
            final int[] docIdToGroup = new int[100];
            session.exportDocIdToGroupId(docIdToGroup);
            assertEquals(Ints.asList(IntStream.generate(() -> 1).limit(100).toArray()), Ints.asList(docIdToGroup));
        }

        final DateTime noonShardStart = queryStart.plusHours(12);
        final DateTime noonShardEnd = queryStart.plusHours(13);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, new Interval(noonShardStart, noonShardEnd))) {
            assertEquals(14, session.metricRegroup(Collections.singletonList("unixtime"), queryStart.getMillis()/1000, queryEnd.getMillis()/1000, 3600));
            assertTrue(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS) instanceof ConstantGroupLookup);
            final int[] docIdToGroup = new int[100];
            session.exportDocIdToGroupId(docIdToGroup);
            assertEquals(Ints.asList(IntStream.generate(() -> 13).limit(100).toArray()), Ints.asList(docIdToGroup));
        }

        try {
            try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, new Interval(noonShardStart, noonShardEnd))) {
                session.metricRegroup(Collections.singletonList("unixtime"), queryStart.getMillis()/1000, queryEnd.getMillis()/1000, 1800);
            }
            fail("Expected failure reading unixtime files");
        } catch (Exception ignored) {
        }

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, new Interval(noonShardStart, noonShardEnd))) {
            assertTrue(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS) instanceof ConstantGroupLookup);

            assertEquals(2, session.regroup(new QueryRemapRule(1, Query.newTermQuery(Term.stringTerm("testField", "term")), 0, 1)));
            assertTrue(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS) instanceof BitSetGroupLookup);
            Assert.assertArrayEquals(new long[]{0, 4}, session.getGroupStats(Collections.singletonList("count()")));

            assertEquals(14, session.metricRegroup(Collections.singletonList("unixtime"), queryStart.getMillis() / 1000, queryEnd.getMillis() / 1000, 3600));

            assertTrue(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS) instanceof BitSetGroupLookup);

            final int[] expectedGroups = new int[100];
            expectedGroups[0] = 13;
            expectedGroups[5] = 13;
            expectedGroups[20] = 13;
            expectedGroups[53] = 13;
            final int[] actualGroups = new int[100];
            session.exportDocIdToGroupId(actualGroups);
            Assert.assertArrayEquals(expectedGroups, actualGroups);

            // Ensure that FTGS iterator with no stats works correctly (see IMTEPD-512)
            try (final FTGSIterator ftgsIterator = session.getFTGSIterator(new String[0], new String[]{"testField"}, Collections.emptyList())) {
                assertTrue(ftgsIterator.nextField());
                assertTrue(ftgsIterator.nextTerm());
                assertEquals("term", ftgsIterator.termStringVal());
                assertTrue(ftgsIterator.nextGroup());
                assertEquals(13, ftgsIterator.group());
                assertFalse(ftgsIterator.nextGroup());
                assertFalse(ftgsIterator.nextTerm());
                assertFalse(ftgsIterator.nextField());
            }
        }
    }

    private static MockFlamdexReader newMetricRegroupTestReader() {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      singletonList("sf1"),
                                      singletonList("if1"), 10, shardDir);
        r.addIntTerm("if1", 5, Arrays.asList(0, 2, 4, 6, 8));
        r.addIntTerm("if1", 10, Arrays.asList(1, 7));
        r.addIntTerm("if1", 15, Arrays.asList(3, 5));
        r.addIntTerm("if1", 20, singletonList(9));
        r.addStringTerm("sf1", "☃", Arrays.asList(3, 4, 8));
        return r;
    }

    private static MockFlamdexReader new2DMetricRegroupTestReader() {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2", "if3"),
                                      singletonList("sf1"),
                                      Arrays.asList("if1", "if2", "if3"), 10, shardDir);

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
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.stringOrRegroup("sf4",
                    new String[]{"asdf", "cdef"},
                    (char) 1,
                    (char) 0,
                    (char) 1);
            final long[] stats = session.getGroupStats(singletonList("count()"));
            assertEquals(6, stats[1]);
        }
    }

    @Test
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.metricRegroup(singletonList("docId()"), (long) 0, session.getNumDocs(), (long) 1);
            final ArrayList<String> docIdPlusCount = Lists.newArrayList("docId()", "count()", "+");
            assertArrayEquals(new long[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20}, session.getGroupStats(docIdPlusCount));
            assertEquals(21, session.regroup(new int[] {10, 11, 12}, new int[]{0, 1, 2}, false));
            assertArrayEquals(new long[] {0,12,14,3,4,5,6,7,8,9,0,0,0,13,14,15,16,17,18,19,20}, session.getGroupStats(docIdPlusCount));
            assertEquals(5, session.regroup(new int[] {2, 6, 7}, new int[]{2, 3, 4}, true));
            assertArrayEquals(new long[] {0,0,14,6,7}, session.getGroupStats(docIdPlusCount));
        }
    }

    @Test
    public void testStuff() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{
                            new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("if3", true, 9999, null, false)})
                    });
            session.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{
                            new RegroupCondition("if3", true, 19, null, false)
                    }),
                    new GroupMultiRemapRule(2, 3, new int[]{4}, new RegroupCondition[]{new RegroupCondition("sf2", false, 0, "b", false)})
            });
            final long[] stats = session.getGroupStats(singletonList("count()"));
            assertEquals(10, stats[1]);
            assertEquals(5, stats[2]);
            assertEquals(4, stats[3]);
            assertEquals(1, stats[4]);
        }
    }

    private static int getGroupIndex(final double value, final double[] percentages) {
        final IterativeHasherUtils.GroupChooser chooser =
                IterativeHasherUtils.createChooser(percentages);
        final int hash = IterativeHasherUtils.percentileToThreshold (value);
        return chooser.getGroup(hash);
    }

    @Test
    public void testRandomMetricMultiRegroupIndexCalculation() throws ImhotepOutOfMemoryException {

        // normal case -- 0.5 falls in the [0.4, 0.7) bucket, which is the
        // fourth (index == 3) in the list of:
        // [0.0, 0.1)
        // [0.1, 0.3)
        // [0.3, 0.4)
        // [0.4, 0.7)
        // [0.7, 0.9)
        // [0.9, 1.0]
        assertEquals(3, getGroupIndex(0.5, new double[]{0.1, 0.3, 0.4, 0.7, 0.9}));

        // less than all
        assertEquals(0, getGroupIndex(0.0, new double[]{0.1, 0.2, 0.3}));

        // empty array
        assertEquals(0, getGroupIndex(0.5, new double[]{}));

        // less than last element
        assertEquals(3, getGroupIndex(0.8, new double[]{0.1, 0.4, 0.5, 0.9}));

        // greater than all
        assertEquals(4, getGroupIndex(0.95, new double[]{0.1, 0.4, 0.5, 0.9}));

        // same as above but make sure than MultiGroupChooser is created
        // to do so make one of values have 6 digits after comma.
        final double x = 1e-6;
        assertEquals(3, getGroupIndex(0.5, new double[]{0.1+x, 0.3, 0.4, 0.7, 0.9}));
        assertEquals(0, getGroupIndex(0.0, new double[]{0.1+x, 0.2, 0.3}));
        assertEquals(0, getGroupIndex(0.5, new double[]{}));
        assertEquals(3, getGroupIndex(0.8, new double[]{0.1+x, 0.4, 0.5, 0.9}));
        assertEquals(4, getGroupIndex(0.95, new double[]{0.1+x, 0.4, 0.5, 0.9}));
        assertEquals(4, getGroupIndex(0.9, new double[]{0.1+x, 0.4, 0.5, 0.9}));
    }

    @Test
    public void testRandomMetricMultiRegroup_ensureValidMultiRegroupArrays() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {

            // ******************************** Stuff that's OK:
            // normal case
            session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.5, 0.7, 0.9},
                    new int[]{1, 2, 3, 4, 5});

            // ******************************** Stuff that's not OK:
            // Bad lengths
            try {
                session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.2, 0.3}, new int[]{1, 2,
                        3});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected
            try {
                session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.5, 0.7}, new int[]{1, 2,
                        3, 4,
                        5, 6});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected
            try {
                session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.3, 0.5, 0.7, 0.9},
                        new int[]{1, 2, 3});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected

            // Percentages not in order
            try {
                session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.5, 0.3}, new int[]{1, 2,
                        3, 4});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected

            // Percentages out of bounds
            try {
                session.ensureValidMultiRegroupArrays(new double[]{-0.001, 0.1, 0.5, 0.7},
                        new int[]{1, 2, 3, 4, 5});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected
            try {
                session.ensureValidMultiRegroupArrays(new double[]{0.1, 0.5, 0.7, 1.00001},
                        new int[]{1, 2, 3, 4, 5});
                fail("ensureValidMultiRegroupArrays didn't throw IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
            } // expected
        }
    }

    private boolean testRandomMetricMultiRegroup(
                                final int groupCount,
                                final int groupSize,
                                final String salt,
                                final double maxError) throws ImhotepOutOfMemoryException {
        final FlamdexReader r = new MemoryFlamdex().setNumDocs(groupCount * groupSize);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final double[] p = new double[groupCount-1];
            for (int i = 0; i < (groupCount-1); i++) {
                p[i] = ((double)i+1)/groupCount;
            }
            final int[] groups = new int[groupCount];
            for (int i = 0; i < groupCount; i++) {
                groups[i] = i + 2;
            }
            session.randomMetricMultiRegroup(singletonList("docId()"), salt, 1, p, groups);
            final long[] stats = session.getGroupStats(singletonList("count()"));
            for (final int group : groups) {
                final long stat = stats[group];
                if (Math.abs(stat - groupSize) > (maxError * groupSize)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Test
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException {
        assertTrue(testRandomMetricMultiRegroup(10, 100000, "g;slkdjglskdfj", 0.01));
        assertTrue(testRandomMetricMultiRegroup(10, 1000000, "s;lgjsldkfjslfjk", 0.01));
        assertTrue(testRandomMetricMultiRegroup(10, 10000000, "lskdgjlskfjslkdfj", 0.01));
    }

    @Test
    public void testSingleMultisplitIntRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 11, shardDir);
        for (int i = 1; i <= 10; i++) {
            final List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
            }
            r.addIntTerm("if1", i, l);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final RegroupCondition[] conditions = new RegroupCondition[10];
            final int[] positiveGroups = new int[10];
            for (int i = 1; i <= 10; i++) {
                conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
                positiveGroups[i - 1] = i;
            }
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(1, 0, positiveGroups,
                    conditions)});
            final int[] docIdToGroup = new int[11];
            session.exportDocIdToGroupId(docIdToGroup);
            for (int docId = 0; docId < 10; docId++) {
                final int actualGroup = docIdToGroup[docId];
                final int expectedGroup = docId + 1;
                assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
            }
            assertEquals("doc id #10 should be in no group", 0, docIdToGroup[10]);
        }
    }

    @Test
    public void testSingleMultisplitStringRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Collections.<String>emptyList(),
                                      singletonList("sf1"),
                                      Collections.<String>emptyList(), 11, shardDir);
        for (int i = 1; i <= 10; i++) {
            final List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
            }
            r.addStringTerm("sf1", String.valueOf(i), l);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            final RegroupCondition[] conditions = new RegroupCondition[10];
            final int[] positiveGroups = new int[10];
            for (int i = 1; i <= 10; i++) {
                conditions[i - 1] = new RegroupCondition("sf1", false, 0, String.valueOf(i), false);
                positiveGroups[i - 1] = i;
            }
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(1, 0, positiveGroups,
                    conditions)});
            final int[] docIdToGroup = new int[11];
            session.exportDocIdToGroupId(docIdToGroup);
            for (int docId = 0; docId < 10; docId++) {
                final int actualGroup = docIdToGroup[docId];
                final int expectedGroup = docId + 1;
                assertEquals("doc id #" + docId + " was misgrouped;", expectedGroup, actualGroup);
            }
            assertEquals("doc id #10 should be in no group", 0, docIdToGroup[10]);
        }
    }

    @Test
    public void testParallelMultisplitIntRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 22, shardDir);
        for (int i = 1; i <= 10; i++) {
            final List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
                l.add(10 + (j - 1));
            }
            r.addIntTerm("if1", i, l);
        }

        // Add 0-9 to if2 so we can split it out
        final List<Integer> l = Lists.newArrayList();
        for (int i = 0; i < 11; i++) {
            l.add(10 + i);
        }
        r.addIntTerm("if2", 0, l);

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, null)),
                    1, 2));
            final int[] positiveGroups = new int[10];
            final RegroupCondition[] conditions = new RegroupCondition[10];
            for (int i = 1; i <= 10; i++) {
                positiveGroups[i - 1] = i;
                conditions[i - 1] = new RegroupCondition("if1", true, i, null, false);
            }
            session.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 0, positiveGroups,
                            conditions),
                    new GroupMultiRemapRule(2, 0, positiveGroups,
                            conditions)});
            final int[] docIdToGroup = new int[22];
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
    }

    @Test
    public void testParallelMultisplitStringRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Collections.<String>emptyList(),
                                      Arrays.asList("sf1", "sf2"),
                                      Collections.<String>emptyList(), 22, shardDir);
        for (int i = 1; i <= 10; i++) {
            final List<Integer> l = Lists.newArrayList();
            for (int j = 1; j <= i; j++) {
                l.add(j - 1);
                l.add(10 + (j - 1));
            }
            r.addStringTerm("sf1", String.valueOf(i), l);
        }

        // Add 0-9 to if2 so we can split it out
        final List<Integer> l = Lists.newArrayList();
        for (int i = 0; i < 11; i++) {
            l.add(10 + i);
        }
        r.addStringTerm("sf2", "0", l);

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new QueryRemapRule(1, Query.newTermQuery(new Term("sf2", false, 0, "0")),
                    1, 2));
            final int[] positiveGroups = new int[10];
            final RegroupCondition[] conditions = new RegroupCondition[10];
            for (int i = 1; i <= 10; i++) {
                positiveGroups[i - 1] = i;
                conditions[i - 1] = new RegroupCondition("sf1", false, 0, String.valueOf(i), false);
            }
            session.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(1, 0, positiveGroups,
                            conditions),
                    new GroupMultiRemapRule(2, 0, positiveGroups,
                            conditions)});
            final int[] docIdToGroup = new int[22];
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
    }

    @Test
    public void testMultisplitTargetingNonexistentGroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 11, shardDir);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1000,
                    1234,
                    new int[]{1},
                    new RegroupCondition[]{new RegroupCondition(
                            "if1",
                            true,
                            1,
                            null,
                            false)})});
        }
    }

    @Test
    public void testIntMultiInequalitySplit() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 10, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
            r.addIntTerm("if2", i, i);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    5,
                    new int[]{1, 2, 3},
                    new RegroupCondition[]{
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
                    })});
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
    }

    @Test
    public void testMultisplitGeneralInputValidation() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        // count mismatch #1
        {
            final MockFlamdexReader r =
                    new MockFlamdexReader(singletonList("if1"),
                                          Collections.<String>emptyList(),
                                          Collections.<String>emptyList(), 10, shardDir);
            try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
                try {
                    session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                            1,
                            0,
                            new int[]{1},
                            new RegroupCondition[]{
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
                                            true),})});
                    fail("Improperly handles having more conditions than positive groups");
                } catch (final IllegalArgumentException e) {
                }
            }
        }

        // count mismatch #2
        {
            final MockFlamdexReader r =
                    new MockFlamdexReader(singletonList("if1"),
                                          Collections.<String>emptyList(),
                                          Collections.<String>emptyList(), 10, shardDir);
            try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
                try {
                    session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                            1,
                            0,
                            new int[]{1,
                                    2},
                            new RegroupCondition[]{new RegroupCondition(
                                    "if1",
                                    true,
                                    1,
                                    null,
                                    true),})});
                    fail("Improperly handles having fewer conditions than positive groups");
                } catch (final IllegalArgumentException e) {
                }
            }
        }

    }

    @Test
    public void testIntMultisplitInequalityInputValidation() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 10, shardDir);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            try {
                session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                        1,
                        5,
                        new int[]{1, 2},
                        new RegroupCondition[]{
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
                                        true),})});
                fail("Improperly handles unreachable inequality splits");
            } catch (final IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testStringMultisplitInequalityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = singletonList("sf1");
        final List<String> emptyList = Collections.emptyList();
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10, shardDir);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            try {
                session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                        1,
                        5,
                        new int[]{1, 2},
                        new RegroupCondition[]{
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
                                        true),})});
                fail("Improperly handles unreachable inequality splits");
            } catch (final IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testStringMultisplitEqualityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = singletonList("sf1");
        final List<String> emptyList = Collections.emptyList();
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10, shardDir);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            // verify doesn't fail
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    5,
                    new int[]{1, 2},
                    new RegroupCondition[]{
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

                    })});
            try {
                session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                        1,
                        5,
                        new int[]{1, 2},
                        new RegroupCondition[]{
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
                                        false)})});
                fail("Improperly handles unreachable equality splits");
            } catch (final IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testIntMultisplitEqualityInputValidation() throws ImhotepOutOfMemoryException {
        final List<String> fields = singletonList("if1");
        final List<String> emptyList = Collections.emptyList();
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(fields, emptyList, emptyList, 10, shardDir);
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            // verify doesn't fail
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    5,
                    new int[]{1, 2},
                    new RegroupCondition[]{
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

                    })});
            try {
                session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                        1,
                        5,
                        new int[]{1, 2},
                        new RegroupCondition[]{
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
                                        false)})});
                fail("Improperly handles unreachable equality splits");
            } catch (final IllegalArgumentException e) {
            }
        }
    }

    @Test
    public void testIntMultiParallelInequalitySplit() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(Arrays.asList("if1", "if2"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 20, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i, i + 10);
        }
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if2", 1, Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    1,
                    new int[]{2},
                    new RegroupCondition[]{new RegroupCondition(
                            "if2",
                            true,
                            1,
                            null,
                            false)})});
            session.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(
                            1,
                            5,
                            new int[]{1, 2, 3},
                            new RegroupCondition[]{
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
                                            true),}),
                    new GroupMultiRemapRule(
                            2,
                            10,
                            new int[]{6, 7, 8},
                            new RegroupCondition[]{
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
                                            true),})});
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
    }

    @Test
    public void testStringMultiInequalitySplit() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1", "sf2");
        final List<String> emptyList = Collections.emptyList();
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(emptyList, fields, emptyList, 10, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf1", String.valueOf(i), i);
            r.addStringTerm("sf2", String.valueOf(i), i);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    5,
                    new int[]{1, 2, 3},
                    new RegroupCondition[]{
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
                                    true),})});
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
    }

    @Test
    public void testStringMultiParallelInequalitySplit() throws ImhotepOutOfMemoryException {
        final List<String> fields = Arrays.asList("sf1", "sf2");
        final List<String> empty = Collections.emptyList();
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(empty, fields, empty, 20, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf1", String.valueOf(i), i, i + 10);
        }
        for (int i = 0; i < 10; i++) {
            r.addStringTerm("sf2", "1", Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    1,
                    new int[]{2},
                    new RegroupCondition[]{new RegroupCondition(
                            "sf2",
                            false,
                            0,
                            "1",
                            false)})});
            session.regroup(new GroupMultiRemapRule[]{
                    new GroupMultiRemapRule(
                            1,
                            5,
                            new int[]{1, 2, 3},
                            new RegroupCondition[]{
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
                                            true),}),
                    new GroupMultiRemapRule(
                            2,
                            10,
                            new int[]{6, 7, 8},
                            new RegroupCondition[]{
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
                                            true),})});
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
    }

    @Test
    public void testManyGroupMultiRemapRuleThings() throws ImhotepOutOfMemoryException {
        final List<String> intFields = Arrays.asList("if1", "if2");
        final List<String> stringFields = Arrays.asList("sf1", "sf2");
        final List<String> emptyList = Collections.emptyList();
        final int numDocs = 7;
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r = new MockFlamdexReader(intFields, stringFields, emptyList, numDocs, shardDir);
        final int[] i1terms = new int[] { 1, 2, 3, 4, 5, 6 };
        final int[] i2terms = new int[] { 5, 1, 2, 3, 8, 7 };
        final String[] s1terms = new String[] { "e", "d", "c", "bc", "b", "a" };
        final String[] s2terms = new String[] { "foo", "bar", "baz", "foo", "bar", "baz" };
        addIntField(r, "if1", i1terms);
        addIntField(r, "if2", i2terms);
        addStringField(r, "sf1", s1terms);
        addStringField(r, "sf2", s2terms);

        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            testAllInequalitySplits(numDocs, "if1", i1terms, session);
            testAllInequalitySplits(numDocs, "if2", i2terms, session);
            testAllInequalitySplits(numDocs, "sf1", s1terms, session);
            testAllInequalitySplits(numDocs, "sf2", s2terms, session);

            final int[] docIdToGroup = new int[numDocs];

            // Try parallel inequality regroups, verify that later ones do not
            // override earlier ones.
            session.resetGroups();
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    2,
                    new int[]{1, 3},
                    new RegroupCondition[]{
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
                                    true)})});
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
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    2,
                    new int[]{1, 3},
                    new RegroupCondition[]{
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
                                    true)})});
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
    }

    @Test
    public void testEmptyMultisplit() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 10, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    1,
                    2,
                    new int[]{1},
                    new RegroupCondition[]{new RegroupCondition(
                            "if1",
                            true,
                            5,
                            null,
                            true)})});
            session.regroup(new GroupMultiRemapRule[]{});
            final int[] docIdToGroup = new int[10];
            session.exportDocIdToGroupId(docIdToGroup);
            for (final int group : docIdToGroup) {
                assertEquals(0, group);
            }
        }
    }

    @Test
    public void testUntargetedGroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final MockFlamdexReader r =
                new MockFlamdexReader(singletonList("if1"),
                                      Collections.<String>emptyList(),
                                      Collections.<String>emptyList(), 10, shardDir);
        for (int i = 0; i < 10; i++) {
            r.addIntTerm("if1", i, i);
        }
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.regroup(new GroupMultiRemapRule[]{new GroupMultiRemapRule(
                    2,
                    3,
                    new int[]{1},
                    new RegroupCondition[]{new RegroupCondition(
                            "if1",
                            true,
                            1,
                            null,
                            false)})});
            final int[] docIdToGroup = new int[10];
            session.exportDocIdToGroupId(docIdToGroup);
            for (final int group : docIdToGroup) {
                assertEquals(0, group);
            }
        }
    }

    private void testAllInequalitySplits(final int numDocs,
                                         final String field,
                                         final int[] terms,
                                         final ImhotepLocalSession session) throws ImhotepOutOfMemoryException {
        testTermInequalitySplit(numDocs, field, terms, session, Integer.MIN_VALUE);
        testTermInequalitySplit(numDocs, field, terms, session, Integer.MAX_VALUE);
        for (final int term : terms) {
            testTermInequalitySplit(numDocs, field, terms, session, term);
            testTermInequalitySplit(numDocs, field, terms, session, term - 1);
            testTermInequalitySplit(numDocs, field, terms, session, term + 1);
        }
    }

    private void testTermInequalitySplit(final int numDocs,
                                         final String field,
                                         final int[] terms,
                                         final ImhotepLocalSession session,
                                         final int term) throws ImhotepOutOfMemoryException {
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

    private void testAllInequalitySplits(final int numDocs,
                                         final String field,
                                         final String[] terms,
                                         final ImhotepLocalSession session) throws ImhotepOutOfMemoryException {
        testTermInequalitySplit(numDocs, field, terms, session, "");
        for (final String term : terms) {
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

    private void testTermInequalitySplit(final int numDocs,
                                         final String field,
                                         final String[] terms,
                                         final ImhotepLocalSession session,
                                         final String term) throws ImhotepOutOfMemoryException {
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

    @Test
    public void testEmptyConditionsRegroup() throws ImhotepOutOfMemoryException {
        final Path shardDir = TestFileUtils.createTempShard();
        final int numDocs = 1000;
        final MockFlamdexReader r =
                new MockFlamdexReader(
                        Collections.emptyList(),
                        Collections.<String>emptyList(),
                        Collections.<String>emptyList(),
                        numDocs,
                        shardDir);
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            // rule without conditions. we expect all docs to regroup in negative group (2)
            final GroupMultiRemapRule rule =
                    new GroupMultiRemapRule(1, 2, new int[0], new RegroupCondition[0]);
            session.regroup(new GroupMultiRemapRule[]{rule});
            final int[] docIdToGroup = new int[numDocs];
            session.exportDocIdToGroupId(docIdToGroup);
            // check all docs are in group 2
            for (int docId = 0; docId < numDocs; docId++) {
                assertEquals(2, docIdToGroup[docId]);
            }
        }
    }

    private void addIntField(final MockFlamdexReader r, final String fieldName, final int[] terms) {
        final Map<Integer, List<Integer>> map = Maps.newHashMap();
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

    private void addStringField(final MockFlamdexReader r, final String fieldName, final String[] terms) {
        final Map<String, List<Integer>> map = Maps.newHashMap();
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
    public void testPushStatFloatScale() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            // like iplat
            final long[] stats = session.getGroupStats(singletonList("floatscale floatfield*100+9000"));
            final long scaledSum = stats[1];
            // we have 5 documents for each of 4 values: 1.5, 2.5, 0 and 18000
            final long expectedSum =
                    (long) ((1.5 * 100 + 9000) + (2.5 * 100 + 9000) + (0 * 100 + 9000) + (18000 * 100 + 9000)) * 5;
            assertEquals("Sum of scaled values", expectedSum, scaledSum, 0.001);
        }
    }

    @Test
    public void testPushStatLen() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {

            final long[] stats = session.getGroupStats(singletonList("len sf1"));
            // 37 == "a".length() * 4 + "hello world".length() * 3
            assertEquals( "Sum of len for 'sf1' field", 37, stats[1]);

            boolean hasException = false;
            try {
                // 'sf4' is multi-valued field for docId == 6, we expect exception to be thrown.
                session.getGroupStats(singletonList("len sf4"));
            } catch ( final Throwable t ) {
                hasException = true;
            }
            Assert.assertTrue("Exception must be thrown for multi-valued field", hasException);
        }
    }

    @Test
    public void testPushStatTermCount() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {

            session.metricRegroup(singletonList("docId()"), (long) 0, (long) session.numDocs, (long) 1, true);

            // inttermcount intfield
            final long[] stats0 = session.getGroupStats(singletonList("inttermcount if2"));
            // strtermcount strfield
            final long[] stats1 = session.getGroupStats(singletonList("strtermcount sf4"));
            // check that strtermcount intfield is zero
            final long[] stats2 = session.getGroupStats(singletonList("strtermcount if2"));
            // check only convertible strings are counted
            final long[] stats3 = session.getGroupStats(singletonList("inttermcount floatfield"));

            assertArrayEquals(new long[] {0, 1, 2, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 2, 0, 1, 1, 1, 1, 1}, stats0);
            assertArrayEquals(new long[] {0, 0, 0, 1, 1, 1, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1}, stats1);
            assertArrayEquals(new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, stats2);
            assertArrayEquals(new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, stats3);
        }
    }

    @Test
    public void testPushStatUidToUnixtime() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.metricRegroup(singletonList("docId()"), (long) 0, (long) session.numDocs, (long) 1, true);
            final long[] stats = session.getGroupStats(singletonList("uid_to_unixtime uid"));
            final long a = new DateTime(2019, 4, 17, 6, 33, 24, DateTimeZone.UTC).getMillis() / 1000;
            final long b = new DateTime(2019, 4, 17, 11, 0, 0, DateTimeZone.UTC).getMillis() / 1000;
            final long z = -1;
            assertArrayEquals(new long[]{0,a,a,a,b,b,b,z,z,z,z,z,z,z,z,z,z,z,z,z,z}, stats);
        }
    }

    @Test(expected = MultiValuedFieldUidTimestampException.class)
    public void testPushStatUidToUnixtimeMultivalued() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r, null)) {
            session.getGroupStats(singletonList("uid_to_unixtime uid_multi"));
        }
    }

    @Test
    public void testRegexMetric() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {

            Assert.assertArrayEquals(new long[]{0, 3}, session.getGroupStats(singletonList("regex if1:9000")));
            Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(singletonList("regex if3:.*9")));
            Assert.assertArrayEquals(new long[]{0, 0}, session.getGroupStats(singletonList("regex if3:notaninteger")));
            Assert.assertArrayEquals(new long[]{0, 2}, session.getGroupStats(singletonList("regex sf1:")));
            Assert.assertArrayEquals(new long[]{0, 4}, session.getGroupStats(singletonList("regex sf2:b")));
            Assert.assertArrayEquals(new long[]{0, 10}, session.getGroupStats(singletonList("regex floatfield:[0-9]*\\.[0-9]*")));
            Assert.assertArrayEquals(new long[]{0, 0}, session.getGroupStats(singletonList("regex nonexistent:anything")));
        }
    }

    @Test
    public void testGetPerformanceStats() throws ImhotepOutOfMemoryException, IOException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {

            final PerformanceStats stats = session.getPerformanceStats();
            // It's hard to create reasonable test for getPerformanceStats.
            // Just checking that all values make sence.
            Assert.assertTrue(stats.cpuTime >= 0);
            Assert.assertTrue(stats.maxMemoryUsage >= 0);
            Assert.assertTrue(stats.ftgsTempFileSize >= 0);
            Assert.assertTrue(stats.fieldFilesReadSize >= 0);

        }
    }

    @Test
    public void testCloseAndGetPerformanceStats() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {


            PerformanceStats stats = session.closeAndGetPerformanceStats();
            Assert.assertNotNull(stats);

            // closeAndGetPerformanceStats returns null after session is closed
            stats = session.closeAndGetPerformanceStats();
            Assert.assertNull(stats);

        }
    }

    @Test
    public void testGetDistinct() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {

            {
                final GroupStatsIterator distinct = session.getDistinct("fieldNotExist", true);
                assertTrue(isEqual(distinct, new long[]{0, 0}));
            }

            {
                final GroupStatsIterator distinct = session.getDistinct("if1", true);
                assertTrue(isEqual(distinct, new long[]{0, 4}));
            }

            {
                session.metricRegroup(Lists.newArrayList("if1", "5", "%"), (long) 0, (long) 5, (long) 1);
                final GroupStatsIterator distinct = session.getDistinct("if1", true);
                assertTrue(isEqual(distinct, new long[]{0, 1, 1, 1, 1}));
                session.resetGroupsTo(ImhotepSession.DEFAULT_GROUPS, 1);
            }

            {
                session.metricRegroup(Lists.newArrayList("if1", "2", "%"), (long) 0, (long) 5, (long) 1);
                final GroupStatsIterator distinct = session.getDistinct("if1", true);
                assertTrue(isEqual(distinct, new long[]{0, 1, 3}));
                session.resetGroupsTo(ImhotepSession.DEFAULT_GROUPS, 1);
            }

            {
                session.metricRegroup(singletonList("docId()"), (long) 0, (long) 20, (long) 1);
                final GroupStatsIterator distinct = session.getDistinct("sf2", false);
                assertTrue(isEqual(distinct, new long[]{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1}));
                session.resetGroupsTo(ImhotepSession.DEFAULT_GROUPS, 1);
            }

        }
    }

    @Test
    public void testConsolidateGroups() throws IOException, ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {

            // keep documents {0,1,2,3,4,5,6,7}
            // which have
            // a: 0,0,1,1,0,0,1,1
            // b: 0,1,0,1,0,1,0,1
            // c: 0,0,0,0,1,1,1,1

            session.metricFilter(RegroupParams.DEFAULT, Collections.singletonList("docId()"), 0, 7, false);

            session.regroup(
                    new RegroupParams(ImhotepSession.DEFAULT_GROUPS, "a"),
                    new QueryRemapRule(1, Query.newTermQuery(Term.intTerm("booleanCombinations1", 1)), 0, 1)
            );

            session.regroup(
                    new RegroupParams(ImhotepSession.DEFAULT_GROUPS, "b"),
                    new QueryRemapRule(1, Query.newTermQuery(Term.intTerm("booleanCombinations2", 1)), 0, 1)
            );

            session.regroup(
                    new RegroupParams(ImhotepSession.DEFAULT_GROUPS, "c"),
                    new QueryRemapRule(1, Query.newTermQuery(Term.intTerm("booleanCombinations3", 1)), 0, 1)
            );

            final int[] tmpBuf = new int[8];

            {
                session.consolidateGroups(Lists.newArrayList("a", "b"), Operator.AND, "a AND b");
                final GroupLookup lookup = session.namedGroupLookups.get("a AND b");
                lookup.fillDocGrpBufferSequential(0, tmpBuf, 8);
                assertArrayEquals(new int[]{0,0,0,1, 0,0,0,1}, tmpBuf);
            }

            {
                session.consolidateGroups(Lists.newArrayList("a", "b", "c"), Operator.AND, "a AND b AND c");
                final GroupLookup lookup = session.namedGroupLookups.get("a AND b AND c");
                lookup.fillDocGrpBufferSequential(0, tmpBuf, 8);
                assertArrayEquals(new int[]{0,0,0,0, 0,0,0,1}, tmpBuf);
            }

            {
                session.consolidateGroups(Lists.newArrayList("a", "b"), Operator.OR, "a OR b");
                final GroupLookup lookup = session.namedGroupLookups.get("a OR b");
                lookup.fillDocGrpBufferSequential(0, tmpBuf, 8);
                assertArrayEquals(new int[]{0,1,1,1, 0,1,1,1}, tmpBuf);
            }

            {
                session.consolidateGroups(Lists.newArrayList("a", "b", "c"), Operator.OR, "a OR b OR c");
                final GroupLookup lookup = session.namedGroupLookups.get("a OR b OR c");
                lookup.fillDocGrpBufferSequential(0, tmpBuf, 8);
                assertArrayEquals(new int[]{0,1,1,1, 1,1,1,1}, tmpBuf);
            }

            {
                session.consolidateGroups(Lists.newArrayList("a"), Operator.NOT, "NOT a");
                final GroupLookup lookup = session.namedGroupLookups.get("NOT a");
                lookup.fillDocGrpBufferSequential(0, tmpBuf, 8);
                assertArrayEquals(new int[]{1,1,0,0, 1,1,0,0}, tmpBuf);
            }

            try {
                session.consolidateGroups(Collections.emptyList(), Operator.NOT, "whatever");
                Assert.fail("Expected consolidating 0 groups with NOT to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Collections.emptyList(), Operator.OR, "whatever");
                Assert.fail("Expected consolidating 0 groups with OR to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Collections.singletonList("a"), Operator.OR, "whatever");
                Assert.fail("Expected consolidating 1 group with OR to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Collections.emptyList(), Operator.AND, "whatever");
                Assert.fail("Expected consolidating 0 groups with AND to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Collections.singletonList("a"), Operator.AND, "whatever");
                Assert.fail("Expected consolidating 1 group with AND to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Collections.singletonList("foo"), Operator.NOT, "whatever");
                Assert.fail("Expected consolidating non-existent groups to fail");
            } catch (final IllegalArgumentException e) {
            }

            try {
                session.consolidateGroups(Lists.newArrayList("a", "b"), Operator.NOT, "whatever");
                Assert.fail("Expected consolidating multiple groups with NOT to fail");
            } catch (final IllegalArgumentException e) {
            }
         }
    }

    @Test
    public void testDeleteGroups() throws ImhotepOutOfMemoryException {
        final FlamdexReader r = MakeAFlamdex.make();
        try (final ImhotepLocalSession session = new ImhotepJavaLocalSession("testLocalSession", r,
                null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                null)) {
            Assert.assertNotNull(session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS));
            session.deleteGroups(Collections.singletonList(ImhotepSession.DEFAULT_GROUPS));
            try {
                session.namedGroupLookups.get(ImhotepSession.DEFAULT_GROUPS);
                Assert.fail();
            } catch (final IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("The specified groups do not exist"));
            }
        }
    }

    private static boolean isEqual(final GroupStatsIterator iterator, final long[] array) {
        final int commonLen = Math.min(iterator.getNumGroups(), array.length);
        for (int i = 0; i < commonLen; i++) {
            if (!iterator.hasNext()) {
                throw new IllegalStateException();
            }
            if (iterator.nextLong() != array[i]) {
                return false;
            }
        }

        for (int i = commonLen; i < iterator.getNumGroups(); i++ ) {
            if (iterator.nextLong() != 0) {
                return false;
            }
        }

        for (int i = commonLen; i < array.length; i++) {
            if (array[i] != 0) {
                return false;
            }
        }
        return true;
    }
}
