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
        ImhotepSession s = new RemoteImhotepMultiSession(new ImhotepSession[] { s1, s2 }, 8, null, null);
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
