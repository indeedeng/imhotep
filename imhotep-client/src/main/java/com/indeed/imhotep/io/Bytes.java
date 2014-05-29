package com.indeed.imhotep.io;

/**
 * @author jsgroth
 *
 * utility functions for converting to/from byte arrays
 */
public final class Bytes {
    private Bytes() {}

    public static byte[] intToBytes(int x) {
        return new byte[]{(byte)((x>>>24) & 0xFF), (byte)((x>>>16) & 0xFF), (byte)((x>>>8) & 0xFF), (byte)(x & 0xFF)};
    }

    public static int bytesToInt(byte[] b) {
        return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
    }

    public static byte[] longToBytes(long x) {
        return new byte[]{
                (byte)((x>>>56) & 0xFF),
                (byte)((x>>>48) & 0xFF),
                (byte)((x>>>40) & 0xFF),
                (byte)((x>>>32) & 0xFF),
                (byte)((x>>>24) & 0xFF),
                (byte)((x>>>16) & 0xFF),
                (byte)((x>>>8) & 0xFF),
                (byte)(x & 0xFF)
        };
    }

    public static long bytesToLong(byte[] b) {
        return ((b[0] & 0xFFL) << 56) | ((b[1] & 0xFFL) << 48) | ((b[2] & 0xFFL) << 40) | ((b[3] & 0xFFL) << 32) | ((b[4] & 0xFFL) << 24) | ((b[5] & 0xFFL) << 16) | ((b[6] & 0xFFL) << 8) | (b[7] & 0xFFL);
    }
}
