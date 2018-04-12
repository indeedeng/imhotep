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
 package com.indeed.imhotep.io;

/**
 * @author jsgroth
 *
 * utility functions for converting to/from byte arrays
 */
public final class Bytes {
    private Bytes() {}

    public static byte[] intToBytes(final int x) {
        return new byte[]{(byte)((x>>>24) & 0xFF), (byte)((x>>>16) & 0xFF), (byte)((x>>>8) & 0xFF), (byte)(x & 0xFF)};
    }

    public static int bytesToInt(final byte[] b) {
        return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
    }

    public static byte[] longToBytes(final long x) {
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

    public static long bytesToLong(final byte[] b) {
        return ((b[0] & 0xFFL) << 56) | ((b[1] & 0xFFL) << 48) | ((b[2] & 0xFFL) << 40) | ((b[3] & 0xFFL) << 32) | ((b[4] & 0xFFL) << 24) | ((b[5] & 0xFFL) << 16) | ((b[6] & 0xFFL) << 8) | (b[7] & 0xFFL);
    }
}
