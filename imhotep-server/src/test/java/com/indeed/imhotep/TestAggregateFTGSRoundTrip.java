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
 * @author jwolfe
 */
public class TestAggregateFTGSRoundTrip {
    @Test
    public void testRoundTrip() throws IOException {
        final double[] testStats = { -1, 0, 1, 127, 128, 129, -127, -128, -129,
            Float.MIN_VALUE, Float.MAX_VALUE, 1000000000.0, -1000000000.0,
            Double.MIN_VALUE, Double.MAX_VALUE, -1000000000000.0, 1000000000000.0,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN,
            -0.0f
        };

        for (final double stat : testStats) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final FTGSOutputStreamWriter out = new FTGSOutputStreamWriter(baos)) {

                out.switchField("foo", false);
                out.switchBytesTerm("bar".getBytes(), 3, 0);
                out.switchGroup(1);
                out.addStat(stat);
            }

            final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

            try (final InputStreamAggregateFTGSIterator in = new InputStreamAggregateFTGSIterator(bais, 1, 2)) {
                assertTrue(in.nextField());
                assertEquals("foo", in.fieldName());
                assertTrue(in.nextTerm());
                assertEquals("bar", in.termStringVal());
                assertTrue(in.nextGroup());
                assertEquals(1, in.group());
                final double[] buf = new double[1];
                in.groupStats(buf);
                assertEquals(stat, buf[0], 0.0);
            }
        }
    }
}
