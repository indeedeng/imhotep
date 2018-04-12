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
 package com.indeed.imhotep.archive;

import com.google.common.io.ByteStreams;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author jsgroth
 */
public final class ArchiveUtils {
    private ArchiveUtils() {}

    public static MessageDigest getMD5Digest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void streamCopy(final InputStream is, final OutputStream os, long bytesToCopy) throws IOException {
        final byte[] buf = new byte[8192];
        while (bytesToCopy > 0) {
            final int n = (int)Math.min(bytesToCopy, buf.length);
            ByteStreams.readFully(is, buf, 0, n);
            os.write(buf, 0, n);
            bytesToCopy -= n;
        }
        os.flush();
    }

    /**
     * Converts a byte array to a hex string.  The String returned
     * will be of length exactly {@code bytes.length * 2}.
     */
    @Nonnull
    public static String toHex(@Nonnull final byte[] bytes) {
        final StringBuilder buf = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            final String hexDigits = Integer.toHexString((int) b & 0x00ff);
            if (hexDigits.length() == 1) {
                buf.append('0');
            }
            buf.append(hexDigits);
        }
        return buf.toString();
    }
}
