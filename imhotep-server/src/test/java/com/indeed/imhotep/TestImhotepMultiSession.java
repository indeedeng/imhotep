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

import com.google.common.primitives.Longs;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author dwahler
 */
public class TestImhotepMultiSession {
    @Test
    public void testShardGrouping() throws ImhotepOutOfMemoryException {
        final MockFlamdexReader r1 = new MockFlamdexReader(Collections.<String>emptyList(),
                Collections.singletonList("f"), Collections.<String>emptyList(),
                1, TestFileUtils.createTempShard());
        r1.addStringTerm("f", "foo", Collections.singletonList(0));
        r1.addStringTerm("f", "bar", Collections.singletonList(0));

        final MockFlamdexReader r2 = new MockFlamdexReader(Collections.<String>emptyList(),
                Collections.singletonList("f"), Collections.<String>emptyList(),
                1, TestFileUtils.createTempShard());
        r2.addStringTerm("f", "foo", Collections.singletonList(0));
        r2.addStringTerm("f", "baz", Collections.singletonList(0));

        final String sessionId = "TestImhotepMultiSession";
        try (
                final ImhotepLocalSession s1 = new ImhotepJavaLocalSession(sessionId, r1, null);
                final ImhotepLocalSession s2 = new ImhotepJavaLocalSession(sessionId, r2, null);
                final ImhotepSession s = new MTImhotepLocalMultiSession(
                    sessionId,
                    new ImhotepLocalSession[] { s1, s2 },
                    new MemoryReservationContext(new ImhotepMemoryPool(0)),
                    null,
                    "", "", (byte)0)
        ){
            // groups: s1=[1], s2=[1]

            s.regroup(new GroupRemapRule[]{
                    new GroupRemapRule(1, new RegroupCondition("f", false, 0, "baz", false), 2, 3)
            });

            // groups: s1=[2], s2=[3]
            assertEquals(Arrays.asList(0L, 0L, 1L, 1L), Longs.asList(s.getGroupStats(Collections.singletonList("count()"))).subList(0, 4));

            s.regroup(new GroupRemapRule[]{
                    new GroupRemapRule(3, new RegroupCondition("f", false, 0, "foo", false), 4, 5)
            });

            // groups: s1=[0], s2=[5]
            assertEquals(Arrays.asList(0L, 0L, 0L, 0L, 0L, 1L), Longs.asList(s.getGroupStats(Collections.singletonList("count()"))).subList(0, 6));
        }
    }
}
