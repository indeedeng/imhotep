package com.indeed.imhotep.group;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author jsgroth
 */
public class ImhotepChooser {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final String salt;
    private final double p;

    public ImhotepChooser(String salt, double p) {
        this.salt = salt;
        this.p = p;
    }

    public double getValue(String s) {
        final String data = s + "|" + salt;
        long hash;
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data.getBytes(UTF_8));
            byte[] digest = md.digest();
            hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (((long) digest[i]) & 0xFFl);
            }
            hash = Math.abs(hash);
            return (double)hash / Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean choose(String s) {
        return getValue(s) >= p;
    }
}
