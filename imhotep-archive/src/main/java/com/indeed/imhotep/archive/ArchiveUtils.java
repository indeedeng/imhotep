package com.indeed.imhotep.archive;

import com.google.common.io.ByteStreams;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Nonnull;

/**
 * @author jsgroth
 */
public final class ArchiveUtils {
    private ArchiveUtils() {}

    public static MessageDigest getMD5Digest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void streamCopy(InputStream is, OutputStream os, long bytesToCopy) throws IOException {
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
        StringBuilder buf = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            String hexDigits = Integer.toHexString((int) b & 0x00ff);
            if (hexDigits.length() == 1) {
                buf.append('0');
            }
            buf.append(hexDigits);
        }
        return buf.toString();
    }
}
