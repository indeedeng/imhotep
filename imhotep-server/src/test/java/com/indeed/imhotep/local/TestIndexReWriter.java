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

import com.indeed.flamdex.MakeAFlamdex;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.MockFlamdexWriter;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TestFileUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestIndexReWriter {

    @Test
    public void testMergingIntTermDocIterator() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final MockFlamdexReader r1 =
                new MockFlamdexReader(Arrays.asList("if1", "if2", "if3"),
                                      singletonList("sf1"),
                                      Arrays.asList("if1", "if2"),
                                      10,
                                      TestFileUtils.createTempShard());

        r1.addIntTerm("if1", 0, 0);
        r1.addIntTerm("if1", 1, 1);
        r1.addIntTerm("if1", 2, 2);
        r1.addIntTerm("if1", 3, 3);
        r1.addIntTerm("if1", 4, 4);

        r1.addIntTerm("if2", 5, 5);
        r1.addIntTerm("if2", 6, 6);
        r1.addIntTerm("if2", 7, 7);
        r1.addIntTerm("if2", 8, 8);
        r1.addIntTerm("if2", 9, 9);

        r1.addIntTerm("if3", 0, 0);
        r1.addIntTerm("if3", 1, 1);
        r1.addIntTerm("if3", 2, 2);
        r1.addIntTerm("if3", 3, 3);
        r1.addIntTerm("if3", 4, 4);

        r1.addStringTerm("sf1", "☃", Arrays.asList(1, 4, 5, 6, 7));

        /* make session 2 */
        final MockFlamdexReader r2 =
                new MockFlamdexReader(Arrays.asList("if1", "if2"),
                                      singletonList("sf1"),
                                      Arrays.asList("if1", "if2"),
                                      10,
                                      TestFileUtils.createTempShard());
        r2.addIntTerm("if1", 0, 5);
        r2.addIntTerm("if1", 1, 6);
        r2.addIntTerm("if1", 2, 7);
        r2.addIntTerm("if1", 3, 8);
        r2.addIntTerm("if1", 4, 9);

        r2.addIntTerm("if2", 0, 0);
        r2.addIntTerm("if2", 1, 1);
        r2.addIntTerm("if2", 2, 2);
        r2.addIntTerm("if2", 3, 3);
        r2.addIntTerm("if2", 4, 4);

        r2.addStringTerm("sf1", "☃", Arrays.asList(1, 4, 5, 6, 7));

        /* make session 3 */
        final MockFlamdexReader r3 =
                new MockFlamdexReader(Arrays.asList("if1", "if2"),
                                      singletonList("sf1"),
                                      Arrays.asList("if1", "if2"),
                                      5,
                                      TestFileUtils.createTempShard());
        r3.addIntTerm("if2", 0, 0);
        r3.addIntTerm("if2", 1, 1);
        r3.addIntTerm("if2", 2, 2);
        r3.addIntTerm("if2", 3, 3);
        r3.addIntTerm("if2", 4, 4);

        r2.addStringTerm("sf1", "☃", Arrays.asList(1, 4, 5, 6, 7));

        try (final ImhotepJavaLocalSession session1 = new ImhotepJavaLocalSession("testSession", r1, null);
             final ImhotepJavaLocalSession session2 = new ImhotepJavaLocalSession("testSession", r2, null);
             final ImhotepJavaLocalSession session3 = new ImhotepJavaLocalSession("testSession", r3, null);
             final MockFlamdexWriter w = new MockFlamdexWriter();) {
            /* merge sessions */
            final IndexReWriter irw =
                    new IndexReWriter(
                            Arrays.asList(session3, session1, session2),
                            session1,
                            new MemoryReservationContext(
                                    new ImhotepMemoryPool(Long.MAX_VALUE)));
            irw.optimizeIndices(Arrays.asList("if1", "if2"), new ArrayList<String>(), w);

            assertEquals("Merged Int Fields are wrong", Arrays.asList("if1", "if2"), w.getIntFields());
            assertEquals("Merged Int Terms are wrong", Arrays.asList(5, 20), w.getIntTerms().get("if1")
                    .get(0L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(6, 21), w.getIntTerms().get("if1")
                    .get(1L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(7, 22), w.getIntTerms().get("if1")
                    .get(2L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(8, 23), w.getIntTerms().get("if1")
                    .get(3L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(9, 24), w.getIntTerms().get("if1")
                    .get(4L));

            assertEquals("Merged Int Terms are wrong", Arrays.asList(0, 15), w.getIntTerms().get("if2")
                    .get(0L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(1, 16), w.getIntTerms().get("if2")
                    .get(1L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(2, 17), w.getIntTerms().get("if2")
                    .get(2L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(3, 18), w.getIntTerms().get("if2")
                    .get(3L));
            assertEquals("Merged Int Terms are wrong", Arrays.asList(4, 19), w.getIntTerms().get("if2")
                    .get(4L));
            assertEquals("Merged Int Terms are wrong", singletonList(10), w.getIntTerms().get("if2")
                    .get(5L));
            assertEquals("Merged Int Terms are wrong", singletonList(11), w.getIntTerms().get("if2")
                    .get(6L));
            assertEquals("Merged Int Terms are wrong", singletonList(12), w.getIntTerms().get("if2")
                    .get(7L));
            assertEquals("Merged Int Terms are wrong", singletonList(13), w.getIntTerms().get("if2")
                    .get(8L));
            assertEquals("Merged Int Terms are wrong", singletonList(14), w.getIntTerms().get("if2")
                    .get(9L));
        }
    }

    @Test
    public void testMergingStringTermDocIterator() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final MockFlamdexReader r1 =
                new MockFlamdexReader(singletonList("if1"),
                                      Arrays.asList("sf1", "sf2", "sf3"),
                                      singletonList("if1"),
                                      10,
                                      TestFileUtils.createTempShard());

        r1.addStringTerm("sf1", "0", 0);
        r1.addStringTerm("sf1", "1", 1);
        r1.addStringTerm("sf1", "2", 2);
        r1.addStringTerm("sf1", "3", 3);
        r1.addStringTerm("sf1", "4", 4);

        r1.addStringTerm("sf2", "5", 5);
        r1.addStringTerm("sf2", "6", 6);
        r1.addStringTerm("sf2", "7", 7);
        r1.addStringTerm("sf2", "8", 8);
        r1.addStringTerm("sf2", "9", 9);

        r1.addStringTerm("sf3", "0", 0);
        r1.addStringTerm("sf3", "1", 1);
        r1.addStringTerm("sf3", "2", 2);
        r1.addStringTerm("sf3", "3", 3);
        r1.addStringTerm("sf3", "4", 4);

        r1.addIntTerm("if1", 3, Arrays.asList(1, 4, 5, 6, 7));

        /* make session 2 */
        final MockFlamdexReader r2 =
                new MockFlamdexReader(singletonList("if1"),
                                      Arrays.asList("sf1", "sf2"),
                                      singletonList("if1"),
                                      10,
                                      TestFileUtils.createTempShard());
        r2.addStringTerm("sf1", "0", 5);
        r2.addStringTerm("sf1", "1", 6);
        r2.addStringTerm("sf1", "2", 7);
        r2.addStringTerm("sf1", "3", 8);
        r2.addStringTerm("sf1", "4", 9);

        r2.addStringTerm("sf2", "0", 0);
        r2.addStringTerm("sf2", "1", 1);
        r2.addStringTerm("sf2", "2", 2);
        r2.addStringTerm("sf2", "3", 3);
        r2.addStringTerm("sf2", "4", 4);

        r2.addIntTerm("if1", 3, Arrays.asList(1, 4, 5, 6, 7));

        /* make session 3 */
        final MockFlamdexReader r3 =
                new MockFlamdexReader(singletonList("if1"),
                                      singletonList("sf2"),
                                      singletonList("if1"),
                                      5,
                                      TestFileUtils.createTempShard());
        r3.addStringTerm("sf2", "0", 0);
        r3.addStringTerm("sf2", "1", 1);
        r3.addStringTerm("sf2", "2", 2);
        r3.addStringTerm("sf2", "3", 3);
        r3.addStringTerm("sf2", "4", 4);

        r2.addIntTerm("if1", 3, Arrays.asList(1, 4, 5, 6, 7));

        try (final ImhotepJavaLocalSession session1 = new ImhotepJavaLocalSession("testSession", r1, null);
             final ImhotepJavaLocalSession session2 = new ImhotepJavaLocalSession("testSession", r2, null);
             final ImhotepJavaLocalSession session3 = new ImhotepJavaLocalSession("testSession", r3, null);
             final MockFlamdexWriter w = new MockFlamdexWriter()) {

            /* merge sessions */
            final IndexReWriter irw =
                    new IndexReWriter(
                            Arrays.asList(session3, session1, session2),
                            session1,
                            new MemoryReservationContext(
                                    new ImhotepMemoryPool(Long.MAX_VALUE)));
            irw.optimizeIndices(new ArrayList<String>(), Arrays.asList("sf1", "sf2"), w);

            assertEquals("Merged String Fields are wrong",
                    Arrays.asList("sf1", "sf2"),
                    w.getStringFields());
            assertEquals("Merged String Terms are wrong", Arrays.asList(5, 20), w.getStringTerms()
                    .get("sf1").get("0"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(6, 21), w.getStringTerms()
                    .get("sf1").get("1"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(7, 22), w.getStringTerms()
                    .get("sf1").get("2"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(8, 23), w.getStringTerms()
                    .get("sf1").get("3"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(9, 24), w.getStringTerms()
                    .get("sf1").get("4"));

            assertEquals("Merged String Terms are wrong", Arrays.asList(0, 15), w.getStringTerms()
                    .get("sf2").get("0"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(1, 16), w.getStringTerms()
                    .get("sf2").get("1"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(2, 17), w.getStringTerms()
                    .get("sf2").get("2"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(3, 18), w.getStringTerms()
                    .get("sf2").get("3"));
            assertEquals("Merged String Terms are wrong", Arrays.asList(4, 19), w.getStringTerms()
                    .get("sf2").get("4"));
            assertEquals("Merged String Terms are wrong", singletonList(10), w.getStringTerms()
                    .get("sf2").get("5"));
            assertEquals("Merged String Terms are wrong", singletonList(11), w.getStringTerms()
                    .get("sf2").get("6"));
            assertEquals("Merged String Terms are wrong", singletonList(12), w.getStringTerms()
                    .get("sf2").get("7"));
            assertEquals("Merged String Terms are wrong", singletonList(13), w.getStringTerms()
                    .get("sf2").get("8"));
            assertEquals("Merged String Terms are wrong", singletonList(14), w.getStringTerms()
                    .get("sf2").get("9"));
        }
    }

    @Test
    public void testGroupMerging() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final FlamdexReader r1 = MakeAFlamdex.make();
        final ImhotepJavaLocalSession session1 = new ImhotepJavaLocalSession("testSession", r1, null);
        session1.createDynamicMetric("foo");
        session1.createDynamicMetric("bar");
        final int[] bar = {0, 13};
        session1.updateDynamicMetric("bar", bar);
        session1.regroup(new GroupMultiRemapRule[] { new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{
                new RegroupCondition("if3",
                        true,
                        9999,
                        null,
                        false)
        })});
        final int[] foo = {0, 1, 2};
        session1.updateDynamicMetric("foo", foo);
        session1.regroup(new GroupMultiRemapRule[] {
            new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{
                    new RegroupCondition("if3", true, 19, null, false)
            }),
            new GroupMultiRemapRule(2, 3, new int[]{4}, new RegroupCondition[]{
                    new RegroupCondition("sf2", false, 0, "b", false)
            })
        });
        final long[] stats1 = session1.getGroupStats(singletonList("count()"));
        assertEquals(10, stats1[1]);
        assertEquals(5, stats1[2]);
        assertEquals(4, stats1[3]);
        assertEquals(1, stats1[4]);

        /* make session 2 */
        final FlamdexReader r2 = MakeAFlamdex.make();
        final ImhotepJavaLocalSession session2 = new ImhotepJavaLocalSession("testSession", r2, null);
        session2.createDynamicMetric("foo");
        session2.createDynamicMetric("cat");
        final int[] cat = {0, 17};
        session2.updateDynamicMetric("cat", cat);
        session2.regroup(new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{3}, new RegroupCondition[]{
                        new RegroupCondition("if3", true, 9999, null, false)
                })
        });
        final int[] foo2 = {0, 7, 11};
        session2.updateDynamicMetric("foo", foo2);
        session2.regroup(new GroupMultiRemapRule[] {
            new GroupMultiRemapRule(1, 1, new int[]{3}, new RegroupCondition[]{new RegroupCondition("if3", true, 19, null, false)}),
            new GroupMultiRemapRule(3, 2, new int[]{4}, new RegroupCondition[]{new RegroupCondition("sf2", false, 0, "b", false)})
        });
        final long[] stats2 = session2.getGroupStats(singletonList("count()"));
        assertEquals(10, stats2[1]);
        assertEquals(4, stats2[2]);
        assertEquals(5, stats2[3]);
        assertEquals(1, stats2[4]);

        /* merge sessions */
        final FlamdexWriter w = new MockFlamdexWriter();
        final IndexReWriter irw =
                new IndexReWriter(
                                  Arrays.asList(session1, session2),
                                  session1,
                                  new MemoryReservationContext(
                                                               new ImhotepMemoryPool(Long.MAX_VALUE)));
        irw.optimizeIndices(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"), w);
        final GroupLookup gl = irw.getNewGroupLookup();
        gl.recalculateNumGroups();
        assertEquals(5, gl.getNumGroups());
        assertEquals(40, gl.size());
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
            if (i >= 5 && i < 10) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 10 && i < 12) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 12 && i < 13) {
                assertEquals(Integer.toString(i) + " in wrong group", 4,gl.get(i));
            }
            if (i >= 13 && i < 15) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
            if (i >= 20 && i < 25) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
            if (i >= 25 && i < 30) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 30 && i < 32) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 32 && i < 33) {
                assertEquals(Integer.toString(i) + " in wrong group", 4,gl.get(i));
            }
            if (i >= 33 && i < 35) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 35 && i < 40) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
        }
        
        /* check the dynamic metric */
        final Map<String,DynamicMetric> dynamicMetrics = irw.getDynamicMetrics();
        /* check all the groups are there */
        assertEquals(dynamicMetrics.size(), 3);
        assertNotNull(dynamicMetrics.get("foo"));
        assertNotNull(dynamicMetrics.get("bar"));
        assertNotNull(dynamicMetrics.get("cat"));
        
        /* check dynamic metrics per group */
        DynamicMetric dm = dynamicMetrics.get("foo");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 1,dm.lookupSingleVal(i));
            }
            if (i >= 10 && i < 15) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 2,dm.lookupSingleVal(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 1,dm.lookupSingleVal(i));
            }

            if (i >= 20 && i < 30) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 7,dm.lookupSingleVal(i));
            }
            if (i >= 30 && i < 35) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 35 && i < 40) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 7,dm.lookupSingleVal(i));
            }
        }
        
        dm = dynamicMetrics.get("bar");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 13,dm.lookupSingleVal(i));
            }
            if (i >= 20 && i < 40) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
        }
        
        dm = dynamicMetrics.get("cat");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 20 && i < 40) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 17,dm.lookupSingleVal(i));
            }
        }

        session1.close();
        session2.close();
    }


    @Test
    public void testGroup0Filtering() throws ImhotepOutOfMemoryException, IOException {
        /* make session 1 */
        final FlamdexReader r1 = MakeAFlamdex.make();
        final ImhotepJavaLocalSession session1 = new ImhotepJavaLocalSession("testSession", r1, null);
        session1.createDynamicMetric("foo");
        session1.createDynamicMetric("bar");
        final int[] bar = {0, 13};
        session1.updateDynamicMetric("bar", bar);
        session1.regroup(new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{new RegroupCondition("if3", true, 9999, null, false)})
        });
        final int[] foo = {0, 1, 2};
        session1.updateDynamicMetric("foo", foo);
        session1.regroup(new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 0, new int[]{2}, new RegroupCondition[]{new RegroupCondition("if3", true, 19, null, false)}),
                new GroupMultiRemapRule(2, 3, new int[]{4}, new RegroupCondition[]{new RegroupCondition("sf2", false, 0, "b", false)})
        });
        final long[] stats1 = session1.getGroupStats(singletonList("count()"));
        assertEquals(0, stats1[0]);
        assertEquals(0, stats1[1]);
        assertEquals(5, stats1[2]);
        assertEquals(4, stats1[3]);
        assertEquals(1, stats1[4]);

        /* make session 2 */
        final FlamdexReader r2 = MakeAFlamdex.make();
        final ImhotepJavaLocalSession session2 = new ImhotepJavaLocalSession("testSession", r2, null);
        session2.createDynamicMetric("foo");
        session2.createDynamicMetric("cat");
        final int[] cat = {0, 17};
        session2.updateDynamicMetric("cat", cat);
        session2.regroup(new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{3}, new RegroupCondition[]{new RegroupCondition("if3", true, 9999, null, false)})
        });
        final int[] foo2 = {0, 7, 11};
        session2.updateDynamicMetric("foo", foo2);
        session2.regroup(new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 1, new int[]{3}, new RegroupCondition[]{new RegroupCondition("if3", true, 19, null, false)}),
                new GroupMultiRemapRule(3, 2, new int[]{4}, new RegroupCondition[]{new RegroupCondition("sf2", false, 0, "b", false)})
        });
        final long[] stats2 = session2.getGroupStats(singletonList("count()"));
        assertEquals(10, stats2[1]);
        assertEquals(4, stats2[2]);
        assertEquals(5, stats2[3]);
        assertEquals(1, stats2[4]);

        /* merge sessions */
        final FlamdexWriter w = new MockFlamdexWriter();
        final IndexReWriter irw =
                new IndexReWriter(
                                  Arrays.asList(session1, session2),
                                  session1,
                                  new MemoryReservationContext(
                                                               new ImhotepMemoryPool(Long.MAX_VALUE)));
        irw.optimizeIndices(Arrays.asList("if1", "if3"), Arrays.asList("sf1", "sf3", "sf4"), w);
        final GroupLookup gl = irw.getNewGroupLookup();
        gl.recalculateNumGroups();
        assertEquals(5, gl.getNumGroups());
        assertEquals(30, gl.size());
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 5 && i < 7) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 7 && i < 8) {
                assertEquals(Integer.toString(i) + " in wrong group", 4,gl.get(i));
            }
            if (i >= 8 && i < 10) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 10 && i < 15) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " in wrong group", 3,gl.get(i));
            }
            if (i >= 20 && i < 22) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 22 && i < 23) {
                assertEquals(Integer.toString(i) + " in wrong group", 4,gl.get(i));
            }
            if (i >= 23 && i < 25) {
                assertEquals(Integer.toString(i) + " in wrong group", 2,gl.get(i));
            }
            if (i >= 25 && i < 30) {
                assertEquals(Integer.toString(i) + " in wrong group", 1,gl.get(i));
            }
        }

        /* check the dynamic metric */
        final Map<String,DynamicMetric> dynamicMetrics = irw.getDynamicMetrics();
        /* check all the groups are there */
        assertEquals(dynamicMetrics.size(), 3);
        assertNotNull(dynamicMetrics.get("foo"));
        assertNotNull(dynamicMetrics.get("bar"));
        assertNotNull(dynamicMetrics.get("cat"));
        
        /* check dynamic metrics per group */
        DynamicMetric dm = dynamicMetrics.get("foo");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 5) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 1,dm.lookupSingleVal(i));
            }
            if (i >= 5 && i < 7) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 2,dm.lookupSingleVal(i));
            }
            if (i >= 7 && i < 8) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 2,dm.lookupSingleVal(i));
            }
            if (i >= 8 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 2,dm.lookupSingleVal(i));
            }
            if (i >= 10 && i < 15) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 7,dm.lookupSingleVal(i));
            }
            if (i >= 15 && i < 20) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 7,dm.lookupSingleVal(i));
            }
            if (i >= 20 && i < 22) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 22 && i < 23) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 23 && i < 25) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 25 && i < 30) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 7,dm.lookupSingleVal(i));
            }
        }
        
        dm = dynamicMetrics.get("bar");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 13,dm.lookupSingleVal(i));
            }
            if (i >= 10 && i < 30) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
        }
        
        dm = dynamicMetrics.get("cat");
        for (int i = 0; i < gl.size(); i ++) {
            if (i >= 0 && i < 10) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 0,dm.lookupSingleVal(i));
            }
            if (i >= 10 && i < 30) {
                assertEquals(Integer.toString(i) + " has wrong dynamic metric", 17,dm.lookupSingleVal(i));
            }
        }

        session1.close();
        session2.close();
    }

}
