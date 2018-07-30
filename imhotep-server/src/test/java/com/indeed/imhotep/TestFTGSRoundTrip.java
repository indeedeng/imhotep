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

import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author dwahler
 */
public class TestFTGSRoundTrip {
    @Test
    public void testRoundTrip() throws IOException {
        final long[] testStats = { -1, 0, 1, 127, 128, 129, -127, -128, -129,
            Integer.MIN_VALUE, Integer.MAX_VALUE, 1000000000, -1000000000,
            Long.MIN_VALUE, Long.MAX_VALUE, -1000000000000L, 1000000000000L };

        for (final long stat : testStats) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final FTGSOutputStreamWriter out = new FTGSOutputStreamWriter(baos)) {

                out.switchField("foo", false);
                out.switchBytesTerm("bar".getBytes(), 3, 0);
                out.switchGroup(1);
                out.addStat(stat);
            }

            final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            //System.out.println(Utilz.toHex(baos.toByteArray()));

            try (final InputStreamFTGSIterator in = new InputStreamFTGSIterator(bais, 1, 2)) {
                assertTrue(in.nextField());
                assertEquals("foo", in.fieldName());
                assertTrue(in.nextTerm());
                assertEquals("bar", in.termStringVal());
                assertTrue(in.nextGroup());
                assertEquals(1, in.group());
                final long[] buf = new long[1];
                in.groupStats(buf);
                assertEquals(stat, buf[0]);
            }
        }
    }
}
