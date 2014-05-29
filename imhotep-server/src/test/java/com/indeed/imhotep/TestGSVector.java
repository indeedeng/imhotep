package com.indeed.imhotep;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author jplaisance
 */
public final class TestGSVector {
    private static final Logger log = Logger.getLogger(TestGSVector.class);
    @Test
    public void testGSVector() {
        final int[] groupIds = new int[4096];
        for (int i = 0; i < 4096; i++) {
            groupIds[i] = i;
        }
        final Random r = new Random();
        final GSVector[] vectors = new GSVector[8];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new GSVector(5);
        }
        final GSVector merged = new GSVector(5);
        int[] junk = new int[4*1024*1024];
        for (int i = 0; i < junk.length; i++) {
            junk[i] = i;
        }
        final long[] result = new long[4096*5];
        for (int groups = 1; groups <= 4096; groups<<=1) {
            long time = 0;
            for (int loop = 0; loop < 1000; loop++) {
                Arrays.fill(result, 0);
                merged.reset();
                shuffle(groupIds, r);
                for (int i = 0; i < vectors.length; i++) {
                    vectors[i].reset();
                    for (int j = 0; j < groups; j++) {
                        final int group = groupIds[j];
                        for (int k = group*5; k < group*5+5; k++) {
                            final long randomLong = r.nextLong();
                            vectors[i].metrics[k] = randomLong;
                            result[k]+=randomLong;
                        }
                        final int bitset2index = group>>>6;
                        vectors[i].bitset1 |= 1L<<bitset2index;
                        vectors[i].bitset2[bitset2index] |= 1L<<(group&0x3F);
                    }
                    vectors[i].numGroups = groups;
                }
                //shuffle(junk, r);
                time -= System.nanoTime();
                for (int i = 0; i < vectors.length; i++) {
                    merged.merge(vectors[i]);
                }
                time += System.nanoTime();
                for (int i = 0; i < result.length; i++) {
                    if (result[i] != merged.metrics[i]) {
                        System.out.println(i);
                        System.out.println(result[i]);
                        System.out.println(merged.metrics[i]);
                        assertTrue(false);
                    }
                }
            }
            System.out.println(groups+" groups: "+time/1000000d+" us");
        }
    }

    static void shuffle(int[] ar, Random r) {
        for (int i = ar.length - 1; i > 0; i--) {
            final int index = r.nextInt(i + 1);
            final int a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }
}
