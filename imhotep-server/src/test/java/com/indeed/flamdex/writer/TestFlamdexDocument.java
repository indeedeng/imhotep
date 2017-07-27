package com.indeed.flamdex.writer;

import org.junit.Before;
import org.junit.Test;

/**
 * @author vladimir
 */

public class TestFlamdexDocument {
    private String longString;
    @Before
    public void setup() {
        final StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < FlamdexDocument.STRING_TERM_LENGTH_LIMIT; i++) {
            longStringBuilder.append("a");
        }
        longString = longStringBuilder.toString();
    }

    @Test
    public void testStringTermLimits() {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addStringTerm("field", longString);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringTermLimitsTooLong() {
        final FlamdexDocument doc = new FlamdexDocument();
        final String tooLongString = longString + "A";
        doc.addStringTerm("field", tooLongString);
    }
}
