package com.indeed.flamdex.utils;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FlamdexUtilsTest {

    @Test
    public void testCacheRegexIntField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", -501).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", -150).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", -5).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("fieldname", 0, 20).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 150).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("fieldname", 501, 30).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("fieldname", 3551, 40).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 40005).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 3551).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 40005).build());
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);

        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(0);
        expected.set(1);
        expected.set(4);
        expected.set(6);
        expected.set(8);

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheRegexStringField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerms("fieldname", "0", "283").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "3551").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "283").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "3551").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "40005").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "3551").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "40005").build());
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);

        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(4);
        expected.set(6);
        expected.set(8);

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    // for multi valued fields in the equality case it will be true if any value occurs in both docs
    // for multi valued fields for not equals it will be true if no value occurs in both docs
    public void testCacheStringFieldEqual() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("f1", "2").addStringTerm("f2", "1").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerms("f1", "0", "3").addStringTerm("f2", "2").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("f1", "100").addStringTerm("f2", "100").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("f1", "30").addStringTerms("f2", "20", "30").build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(2);
        expected.set(3);

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheIntFieldEqual() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("f1", 1, 3).addIntTerm("f2", 1).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("f1", 0, 4).addIntTerm("f2", 2).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 4).addIntTerm("f2", 0).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("f1", 2, 50).addIntTerm("f2", 50).build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(0);
        expected.set(3);

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheStringFieldEqualEmpty() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("f1", "0").addStringTerm("f2", "2").build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheIntFieldEqualEmpty() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 1).addIntTerm("f2", 3).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 100).addIntTerm("f2", 0).build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(2);
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheFieldEqualNotExists() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 1).addIntTerm("f3", 1).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 100).addStringTerm("f4", "0").build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());

        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheFieldEqualIncompatible() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 1).addStringTerm("f2", "1").build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("f1", 100).addStringTerm("f2", "0").build());

        try {
            FlamdexUtils.cacheFieldEqual("f1", "f2", reader);
        } catch (final IllegalArgumentException e) {
            return;
        }
        fail("field equality between incompatible field is not allowed");
    }


    public void testRegExpCapturedLong() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m151m12").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m151m12").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m151m12").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerms("fieldname", "m0m12", "m283m345").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m3551m678").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m283m345").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m3551m678").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m40005m910").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m3551m678").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "m40005m910").build());

        final int[] docIds = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final long[] metricValues = new long[10];

        FlamdexUtils.cacheRegExpCapturedLong("fieldname", reader, Pattern.compile("m([0-9]+)m([0-9]+)"), 0)
                .lookup(docIds, metricValues, docIds.length);
        assertArrayEquals(new long[]{
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        }, metricValues);

        FlamdexUtils.cacheRegExpCapturedLong("fieldname", reader, Pattern.compile("m([0-5]+)m([0-9]+)"), 1)
                .lookup(docIds, metricValues, docIds.length);
        assertArrayEquals(new long[]{
                151, 151, 151, 0, 3551, 0, 3551, 40005, 3551, 40005
        }, metricValues);

        FlamdexUtils.cacheRegExpCapturedLong("fieldname", reader, Pattern.compile("m([0-9]+)m([0-9]+)"), 1)
                .lookup(docIds, metricValues, docIds.length);
        assertArrayEquals(new long[]{
                151, 151, 151, 283, 3551, 283, 3551, 40005, 3551, 40005
        }, metricValues);

        FlamdexUtils.cacheRegExpCapturedLong("fieldname", reader, Pattern.compile("m([0-9]+)m([0-9]+)"), 2)
                .lookup(docIds, metricValues, docIds.length);
        assertArrayEquals(new long[]{
                12, 12, 12, 345, 678, 345, 678, 910, 678, 910
        }, metricValues);
    }

    @Test
    public void testCacheRegexNoField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheRegexEmptyStringField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 0).build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testCacheRegexEmptyIntField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "str").build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheRegex("fieldname", ".+5.+", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        assertBitsetEquality(expected, bitSet);
    }

    private void assertBitsetEquality(final ThreadSafeBitSet expected, final ThreadSafeBitSet bitSet) {
        // Why can't I just assertEquals on a ThreadSafeBitSet?
        assertEquals(expected.size(), bitSet.size());
        assertEquals(expected.cardinality(), bitSet.cardinality());
        final ThreadSafeBitSet expectedCopy = new ThreadSafeBitSet(expected.size());
        expectedCopy.or(expected);
        expectedCopy.and(bitSet);
        assertEquals("post-AND cardinality mismatch", expectedCopy.cardinality(), bitSet.cardinality());
    }

    @Test
    public void testHasIntField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("fieldname", 0, 151).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 151).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 151).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerms("fieldname", 283, 0).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fake", -1).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 40005).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fake", -1).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 283).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fake", -1).build());
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 40005).build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheHasIntField("fieldname", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(3);
        expected.set(5);
        expected.set(7);
        expected.set(9);
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testHasIntFieldEmptyField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "0").build());
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheHasIntField("fieldname", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testHasStringFieldEmptyField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addIntTerm("fieldname", 0).build());
        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheHasStringField("fieldname", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        assertBitsetEquality(expected, bitSet);
    }

    @Test
    public void testHasStringField() {
        final MemoryFlamdex reader = new MemoryFlamdex();
        reader.addDocument(new FlamdexDocument.Builder().addStringTerms("fieldname", "0", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "151").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerms("fieldname", "283", "0").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fake", "-1").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "40005").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fake", "-1").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "283").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fake", "-1").build());
        reader.addDocument(new FlamdexDocument.Builder().addStringTerm("fieldname", "40005").build());

        final ThreadSafeBitSet bitSet = FlamdexUtils.cacheHasStringField("fieldname", reader);
        final ThreadSafeBitSet expected = new ThreadSafeBitSet(reader.getNumDocs());
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(3);
        expected.set(5);
        expected.set(7);
        expected.set(9);
        assertBitsetEquality(expected, bitSet);
    }
}