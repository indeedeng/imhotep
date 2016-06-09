package com.indeed.flamdex.writer;

import org.junit.Test;

/**
 * @author vladimir
 */

public class TestFlamdexDocument {
    private String longString;
    @org.junit.Before
    public void setup() {
        StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < FlamdexDocument.STRING_TERM_LENGTH_LIMIT; i++) {
            longStringBuilder.append("a");
        }
        longString = longStringBuilder.toString();
    }

    @Test
    public void testStringTermLimits() {
        FlamdexDocument doc = new FlamdexDocument();
        doc.addStringTerm("field", longString);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringTermLimitsTooLong() {
        FlamdexDocument doc = new FlamdexDocument();
        String tooLongString = longString + "A";
        doc.addStringTerm("field", tooLongString);
    }
}
