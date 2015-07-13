package com.indeed.flamdex.utils;

import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Collections;

public class FlamdexUtilsTest extends TestCase {
    public void testCacheRegexIntField() throws Exception {
        final MockFlamdexReader reader = new MockFlamdexReader(Arrays.asList("fieldname"), Collections.<String>emptySet(), Collections.<String>emptySet(), 10);
        reader.addIntTerm("fieldname", 0, 3);
        reader.addIntTerm("fieldname", 151, 0, 1, 2);
        reader.addIntTerm("fieldname", 283, 3, 5);
        reader.addIntTerm("fieldname", 3551, 4, 6, 8);
        reader.addIntTerm("fieldname", 40005, 7, 9);
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);

        final ThreadSafeBitSet expected = new ThreadSafeBitSet(10);
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(4);
        expected.set(6);
        expected.set(8);

        assertBitsetEquality(expected, bitSet);
    }

    public void testCacheRegexStringField() throws Exception {
        final MockFlamdexReader reader = new MockFlamdexReader(Collections.<String>emptySet(), Arrays.asList("fieldname"), Collections.<String>emptySet(), 10);
        reader.addStringTerm("fieldname", "0", 3);
        reader.addStringTerm("fieldname", "151", 0, 1, 2);
        reader.addStringTerm("fieldname", "283", 3, 5);
        reader.addStringTerm("fieldname", "3551", 4, 6, 8);
        reader.addStringTerm("fieldname", "40005", 7, 9);
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);

        final ThreadSafeBitSet expected = new ThreadSafeBitSet(10);
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(4);
        expected.set(6);
        expected.set(8);

        assertBitsetEquality(expected, bitSet);
    }

    public void testCacheRegexNoField() throws Exception {
        final MockFlamdexReader reader = new MockFlamdexReader(Collections.<String>emptySet(), Collections.<String>emptySet(), Collections.<String>emptySet(), 50);
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(50);
        assertBitsetEquality(expected, bitSet);
    }

    public void testCacheRegexEmptyStringField() throws Exception {
        final MockFlamdexReader reader = new MockFlamdexReader(Collections.<String>emptySet(), Arrays.asList("fieldname"), Collections.<String>emptySet(), 50);
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(50);
        assertBitsetEquality(expected, bitSet);
    }

    public void testCacheRegexEmptyIntField() throws Exception {
        final MockFlamdexReader reader = new MockFlamdexReader(Arrays.asList("fieldname"), Collections.<String>emptySet(), Collections.<String>emptySet(), 50);
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(50);
        assertBitsetEquality(expected, bitSet);
    }

    private void assertBitsetEquality(ThreadSafeBitSet expected, ThreadSafeBitSet bitSet) {
        // Why can't I just assertEquals on a ThreadSafeBitSet?
        Assert.assertEquals(expected.size(), bitSet.size());
        Assert.assertEquals(expected.cardinality(), bitSet.cardinality());
        final ThreadSafeBitSet expectedCopy = new ThreadSafeBitSet(expected.size());
        expectedCopy.or(expected);
        expectedCopy.and(bitSet);
        Assert.assertEquals("post-AND cardinality mismatch", expectedCopy.cardinality(), bitSet.cardinality());
    }
}