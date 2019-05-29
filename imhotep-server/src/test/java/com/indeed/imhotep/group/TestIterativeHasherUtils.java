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

package com.indeed.imhotep.group;

import com.indeed.util.core.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestIterativeHasherUtils {
    // we expect these percentiles to be proportional
    private final double[][] goodPercentiles = new double[][] {
            new double[] {0.1, 0.2, 0.3},
            new double[] {((double)2)/7, 0.8},
            new double[] {0.01, 0.75}
    };

    // this is not proportional with maxSize = 256 and maxError = 1e-6
    private final double[][] badPercentiles = new double[][] {
            new double[] {1e-4, 0.2},
            new double[] {1e-3},
            new double[] {Math.PI/4}
    };

    public boolean checkBounds(
            final IterativeHasherUtils.GroupChooser chooser,
            final int groupCount) {
        final int min = chooser.getGroup(Integer.MIN_VALUE);
        final int max = chooser.getGroup(Integer.MAX_VALUE);
        return (min == 0) && (max == (groupCount - 1));
    }

    @Test
    public void testTwoGroupChooser() {

        final int[] thresholds = new int[] {
                Integer.MIN_VALUE + 1,
                -100,
                0,
                100,
                Integer.MAX_VALUE - 1
        };
        for (final int threshold : thresholds) {
            final IterativeHasherUtils.TwoGroupChooser chooser =
                    new IterativeHasherUtils.TwoGroupChooser(threshold);
            assertNotNull(chooser);
            assertTrue(checkBounds(chooser, 2));
            assertEquals(0, chooser.getGroup(threshold-1));
            assertEquals(1, chooser.getGroup(threshold));
            assertEquals(1, chooser.getGroup(threshold+1));
        }
    }

    @Test
    public void testProportionalGroupChooser() {
        for (final double[] p : goodPercentiles) {
            final IterativeHasherUtils.ProportionalMultiGroupChooser chooser =
                    IterativeHasherUtils.ProportionalMultiGroupChooser.tryCreate(p, 1e-6, 256);
            assertNotNull(chooser);
            assertTrue(checkBounds(chooser, p.length+1));
        }

        for (final double[] p : badPercentiles) {
            final IterativeHasherUtils.ProportionalMultiGroupChooser chooser =
                    IterativeHasherUtils.ProportionalMultiGroupChooser.tryCreate(p, 1e-6, 256);
            assertNull(chooser);
        }
    }

    @Test
    public void testMultiGroupChooser() {
        for (final double[] p : (double[][]) ArrayUtils.addAll(goodPercentiles, badPercentiles)) {
            final IterativeHasherUtils.MultiGroupChooser chooser =
                    IterativeHasherUtils.MultiGroupChooser.create(p);
            assertNotNull(chooser);
            assertTrue(checkBounds(chooser, p.length+1));
        }
    }

    @Test
    public void testUniformGroupChooser() {
        final IterativeHasherUtils.GroupChooser chooser4 = IterativeHasherUtils.createUniformChooser(4);
        assertEquals(0, chooser4.getGroup(0));
        assertEquals(1, chooser4.getGroup(1));
        assertEquals(2, chooser4.getGroup(2));
        assertEquals(3, chooser4.getGroup(3));
        assertEquals(0, chooser4.getGroup(4));
        assertEquals(3, chooser4.getGroup(-1));
        assertEquals(0, chooser4.getGroup(160));
        assertEquals(3, chooser4.getGroup(Integer.MAX_VALUE));
        assertEquals(0, chooser4.getGroup(Integer.MIN_VALUE));

        final IterativeHasherUtils.GroupChooser chooser1 = IterativeHasherUtils.createUniformChooser(1);
        assertEquals(0, chooser1.getGroup(0));
        assertEquals(0, chooser1.getGroup(Integer.MAX_VALUE));
        assertEquals(0, chooser1.getGroup(Integer.MIN_VALUE));
        assertEquals(0, chooser1.getGroup(1));
        assertEquals(0, chooser1.getGroup(-1));
        assertEquals(0, chooser1.getGroup(13));
        assertEquals(0, chooser1.getGroup(-13));

        final IterativeHasherUtils.GroupChooser chooser1000 = IterativeHasherUtils.createUniformChooser(1000);
        assertEquals(0, chooser1000.getGroup(0));
        assertEquals(647, chooser1000.getGroup(Integer.MAX_VALUE));
        assertEquals(352, chooser1000.getGroup(Integer.MIN_VALUE));
        assertEquals(1, chooser1000.getGroup(1));
        assertEquals(999, chooser1000.getGroup(-1));
        assertEquals(13, chooser1000.getGroup(13));
        assertEquals(987, chooser1000.getGroup(-13));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUniformGroupChooserInvalidArgument() {
        IterativeHasherUtils.createUniformChooser(-1);
    }
}