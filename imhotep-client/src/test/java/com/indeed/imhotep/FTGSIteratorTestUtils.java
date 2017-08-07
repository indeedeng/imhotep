package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;
import org.junit.Assert;

/**
 * @author kenh
 */

public class FTGSIteratorTestUtils {
    private FTGSIteratorTestUtils() {
    }

    public static void expectIntField(final FTGSIterator iter, final String field) {
        expectFieldEnd(iter);
        Assert.assertTrue(iter.nextField());
        Assert.assertEquals(field, iter.fieldName());
        Assert.assertTrue(iter.fieldIsIntType());
    }

    public static void expectStrField(final FTGSIterator iter, final String field) {
        expectFieldEnd(iter);
        Assert.assertTrue(iter.nextField());
        Assert.assertEquals(field, iter.fieldName());
        Assert.assertFalse(iter.fieldIsIntType());
    }

    public static void expectIntTerm(final FTGSIterator iter, final long term, final long termDocFreq) {
        expectTermEnd(iter);
        Assert.assertTrue(iter.nextTerm());
        Assert.assertEquals(term, iter.termIntVal());
        Assert.assertEquals(termDocFreq, iter.termDocFreq());
    }

    public static void expectStrTerm(final FTGSIterator iter, final String term, final long termDocFreq) {
        expectTermEnd(iter);
        Assert.assertTrue(iter.nextTerm());
        Assert.assertEquals(term, iter.termStringVal());
        Assert.assertEquals(termDocFreq, iter.termDocFreq());
    }

    public static void expectEnd(final FTGSIterator iter) {
        expectFieldEnd(iter);
        Assert.assertFalse(iter.nextField());
    }

    public static void expectFieldEnd(final FTGSIterator iter) {
        expectTermEnd(iter);
        Assert.assertFalse(iter.nextTerm());
    }

    public static void expectGroup(final FTGSIterator iter, final long group, final long[] groupStats) {
        Assert.assertTrue(iter.nextGroup());
        Assert.assertEquals(group, iter.group());

        final long[] stats = new long[groupStats.length];
        iter.groupStats(stats);

        Assert.assertArrayEquals(groupStats, stats);
    }

    public static void expectTermEnd(final FTGSIterator iter) {
        Assert.assertFalse(iter.nextGroup());
    }
}
