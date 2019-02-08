package com.indeed.imhotep.io;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class ImhotepProtobufShippingTest {

    @Test
    public void runLengthEncode() {
        Assert.assertArrayEquals(
                new byte[] {
                        0,0,0,6,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,1, 0,0,0,1,
                        0,0,0,1, 0,0,0,2,
                        0,0,0,1, 0,0,0,3,
                        0,0,0,1, 0,0,0,4,
                        0,0,0,1, 0,0,0,5,
                },
                ImhotepProtobufShipping.runLengthEncode(new int[]{0,1,2,3,4,5})
        );

        Assert.assertArrayEquals(
                new byte[] {
                        0,0,0,6,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,5, 0,0,0,1,
                },
                ImhotepProtobufShipping.runLengthEncode(new int[]{0,1,1,1,1,1})
        );

        Assert.assertArrayEquals(
                new byte[] {
                        0,0,0,13,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,4, 0,0,0,1,
                        0,0,0,4, 0,0,0,2,
                        0,0,0,4, 0,0,0,3,
                },
                ImhotepProtobufShipping.runLengthEncode(new int[]{0,1,1,1,1,2,2,2,2,3,3,3,3})
        );
    }

    @Test
    public void runLengthDecodeIntArray() {
        Assert.assertArrayEquals(
                new int[]{0,1,2,3,4,5},
                ImhotepProtobufShipping.runLengthDecodeIntArray(new byte[] {
                        0,0,0,6,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,1, 0,0,0,1,
                        0,0,0,1, 0,0,0,2,
                        0,0,0,1, 0,0,0,3,
                        0,0,0,1, 0,0,0,4,
                        0,0,0,1, 0,0,0,5,
                })
        );

        Assert.assertArrayEquals(
                new int[]{0,1,1,1,1,1},
                ImhotepProtobufShipping.runLengthDecodeIntArray(new byte[] {
                        0,0,0,6,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,5, 0,0,0,1,
                })
        );

        Assert.assertArrayEquals(
                new int[]{0,1,1,1,1,2,2,2,2,3,3,3,3},
                ImhotepProtobufShipping.runLengthDecodeIntArray(new byte[] {
                        0,0,0,13,
                        0,0,0,1, 0,0,0,0,
                        0,0,0,4, 0,0,0,1,
                        0,0,0,4, 0,0,0,2,
                        0,0,0,4, 0,0,0,3,
                })
        );
    }

    @Test
    public void testRandom() {
        final Random random = new Random(0L);

        for (int i = 0; i < 100; i++) {
            final int length = random.nextInt(10000);
            final int[] array = new int[length];
            for (int j = 0; j < array.length; j++) {
                array[j] = random.nextInt();
            }

            Assert.assertArrayEquals(
                    array,
                    ImhotepProtobufShipping.runLengthDecodeIntArray(ImhotepProtobufShipping.runLengthEncode(array))
            );
        }
    }
}