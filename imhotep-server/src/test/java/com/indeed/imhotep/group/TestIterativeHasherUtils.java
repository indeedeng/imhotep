package com.indeed.imhotep.group;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestIterativeHasherUtils {

    @Test
    public void testProportionalGroupChooser() {

        // we expect these persentiles to be proportional
        final double[][] goodPercentiles = new double[][] {
                new double[] {0.1, 0.2, 0.3},
                new double[] {((double)2)/7, 0.8},
                new double[] {0.01, 0.75}
        };
        for (final double[] p : goodPercentiles) {
            final IterativeHasherUtils.ProportionalMultiGroupChooser chooser =
                    IterativeHasherUtils.ProportionalMultiGroupChooser.tryCreate(p, 1e-6, 256);
            assertNotNull(chooser);
        }

        // this is not proportional with maxSize = 256 and maxError = 1e-6
        final double[][] badPercentiles = new double[][] {
                new double[] {1e-4, 0.2},
                new double[] {1e-3},
                new double[] {Math.PI/4}
        };

        for (final double[] p : badPercentiles) {
            final IterativeHasherUtils.ProportionalMultiGroupChooser chooser =
                    IterativeHasherUtils.ProportionalMultiGroupChooser.tryCreate(p, 1e-6, 256);
            assertNull(chooser);
        }
    }
}