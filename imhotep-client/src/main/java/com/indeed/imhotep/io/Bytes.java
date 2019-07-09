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

import com.google.common.base.Preconditions;

/**
 * @author jsgroth
 *
 * utility functions for converting to/from byte arrays
 */
public final class Bytes {
    private Bytes() {}

    public static void intToBytes(final int x, final byte[] b) {
        Preconditions.checkArgument((b != null) && (b.length == 4), "Need a byte array with length 4");
        b[0] = (byte)((x>>>24));
        b[1] = (byte)((x>>>16));
        b[2] = (byte)((x>>>8));
        b[3] = (byte)(x);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static boolean equals(final byte[] a, final byte[] b, final int length) {
        Preconditions.checkPositionIndex(length, a.length);
        Preconditions.checkPositionIndex(length, b.length);
        for (int i = 0; i < length; ++i) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
}
