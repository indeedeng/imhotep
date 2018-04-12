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
