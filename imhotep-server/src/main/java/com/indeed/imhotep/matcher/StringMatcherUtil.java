package com.indeed.imhotep.matcher;

import com.google.common.base.Preconditions;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;

class StringMatcherUtil {
    private StringMatcherUtil() {
    }

    /**
     * Returns true iff {@code str} is well-formed and can be converted to {@code charset} without anything weird.
     * For example, stray surrogate pair.
     */
    static boolean isWellFormedString(final Charset charset, final String str) {
        try {
            charset
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(str));
            return true;
        } catch (final CharacterCodingException e) {
            return false;
        }

    }

    /**
     * Build KMP table (strict border) for the given pattern
     */
    static int[] buildKMPTable(final byte[] pattern) {
        Preconditions.checkArgument(pattern.length > 0);
        final int[] table = new int[pattern.length + 1];
        table[0] = -1;
        int failureLink = 0;
        for (int i = 1; i < pattern.length; ++i) {
            if (pattern[i] == pattern[failureLink]) {
                table[i] = table[failureLink];
            } else {
                table[i] = failureLink;
                while ((failureLink >= 0) && (pattern[i] != pattern[failureLink])) {
                    failureLink = table[failureLink];
                }
            }
            ++failureLink;
        }
        table[pattern.length] = failureLink;
        return table;
    }
}
