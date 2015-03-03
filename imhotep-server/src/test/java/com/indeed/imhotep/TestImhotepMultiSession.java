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

import com.google.common.primitives.Longs;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.*;

/**
 * @author dwahler
 */
public class TestImhotepMultiSession {
    @Test
    public void testShardGrouping() throws ImhotepOutOfMemoryException {
        MockFlamdexReader r1 = new MockFlamdexReader(Collections.<String>emptyList(),
                Arrays.asList("f"), Collections.<String>emptyList(), 1);
        r1.addStringTerm("f", "foo", Arrays.asList(0));
        r1.addStringTerm("f", "bar", Arrays.asList(0));

        MockFlamdexReader r2 = new MockFlamdexReader(Collections.<String>emptyList(),
                Arrays.asList("f"), Collections.<String>emptyList(), 1);
        r1.addStringTerm("f", "foo", Arrays.asList(0));
        r1.addStringTerm("f", "baz", Arrays.asList(0));

        ImhotepLocalSession s1 = new ImhotepLocalSession(r1), s2 = new ImhotepLocalSession(r2);
        ImhotepSession s = new RemoteImhotepMultiSession(new ImhotepSession[] { s1, s2 }, null, null, -1, null);
        s.pushStat("count()");

        // groups: s1=[1], s2=[1]

        s.regroup(new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("f", false, 0, "baz", false), 2, 3)
        });

        // groups: s1=[2], s2=[3]
        assertEquals(Arrays.asList(0L, 0L, 1L, 1L), Longs.asList(s.getGroupStats(0)).subList(0, 4));

        s.regroup(new GroupRemapRule[] {
                new GroupRemapRule(3, new RegroupCondition("f", false, 0, "foo", false), 4, 5)
        });

        // groups: s1=[0], s2=[5]
        assertEquals(Arrays.asList(0L, 0L, 0L, 0L, 0L, 1L), Longs.asList(s.getGroupStats(0)).subList(0, 6));
        
        s.close();
        s1.close();
        s2.close();
    }
}
