/*
 * Copyright (C) 2014 Indeed Inc.
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

    public ImhotepChooser(final String salt, final double p) {
        this.salt = salt;
        this.p = p;
    }

    public double getValue(final String s) {
        final String data = s + "|" + salt;
        long hash;
        
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data.getBytes(UTF_8));
            final byte[] digest = md.digest();
            hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (((long) digest[i]) & 0xFFL);
            }
            hash = Math.abs(hash);
            return (double)hash / Long.MAX_VALUE;
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean choose(final String s) {
        return getValue(s) >= p;
    }
}
