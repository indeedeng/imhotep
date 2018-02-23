package com.indeed.imhotep.group;

import com.google.common.base.Charsets;
import com.google.common.math.LongMath;

import java.math.RoundingMode;
import java.util.Arrays;

/*
    Class for hashing string and long values.
    We need a separate class instead of some well-known hash function,
    because we want calculate hashes consequently for sorted
    strings or long values.

    The main idea of hasher is following:
    1. We consider input as a sequence of bytes.
    2. We have some hashing function that calculate hash using 4-bytes (int) and some seed value (int)
    3. Input data is splitted in 4-byte chunks and hash value for previous chunk
        is used as seed for next chunk. seed for first chunk is calculated based on user seed string.
    4. This schema allows us to store intermediate results for previously calculated hash and
        skip some calculations if new data has common prefix with previous one.
        Common string prefixes (or common high bits in long values) is main source of performance
        and reason why this class exists.
 */

public abstract class IterativeHasher {

    // Static data for long to string (utf-8) conversion.
    // Max string len for long
    private static final int MAX_LONG_LENGTH = String.valueOf(Long.MIN_VALUE).length();
    private static final byte ZERO = '0';
    private static final byte MINUS = '-';

    private final int seed;

    protected IterativeHasher(final String salt) {
        this.seed = new StringHasher(0).calculateHash(salt);
    }

    public SimpleLongHasher simpleLongHasher() {
        return new SimpleLongHasher(seed);
    }

    public StringHasher stringHasher() {
        return new StringHasher(seed);
    }

    public ConsistentLongHasher consistentLongHasher() {
        return new ConsistentLongHasher(seed);
    }

    public ByteArrayHasher byteArrayHasher() {
        return new ByteArrayHasher(seed);
    }

    public abstract int hashStep(final int value, final int prevHash);

    private static int getStrLen(final long value) {
        // LongMath.log10 expects positive argument.
        if (value > 0) {
            return LongMath.log10(value, RoundingMode.FLOOR) + 1;
        } else if (value < 0) {
            if (value == Long.MIN_VALUE ) {
                // -value for Long.MIN_VALUE is Long.MIN_VALUE
                return MAX_LONG_LENGTH;
            }
            return LongMath.log10(-value, RoundingMode.FLOOR) + 2;
        } else {
            return 1;
        }
    }

    // hasher that represents long as two ints,
    // caches high and low hash and recalculate high only when needed.
    public final class SimpleLongHasher {
        private final int seed;
        private int resultHigh;
        private int resultLow;
        private long lastValue;

        private SimpleLongHasher(final int seed) {
            this.seed = seed;
            resultHigh = hashStep(0, seed);
            resultLow = hashStep(0, resultHigh);
            lastValue = 0;
        }

        public int calculateHash(final long value) {
            if (value == lastValue) {
                return resultLow;
            }

            final long diff = value ^ lastValue;
            if ((diff >> 32) != 0) {
                resultHigh = hashStep((int) (value >>> 32), seed);
            }
            resultLow = hashStep((int)(value), resultHigh);
            lastValue = value;
            return resultLow;
        }
    }

    // Hasher that produces the same result as
    // ByteArrayHasher if long is converted to utf-8 string
    // Class has ad hoc convertion long -> utf-8 for two reason:
    //  1. Much faster that long -> String -> uft-8
    //  2. During convertion we can check if current value has common prefix with last one.
    public final class ConsistentLongHasher {
        // utf-8 representation of current value
        private final byte[] curValue = new byte[MAX_LONG_LENGTH];
        // len of string representation.
        private int curValueLen;
        // state[i] is value of substring curValue[0..i]
        // (except state[0] for negative numbers)
        private final long[] state = new long[MAX_LONG_LENGTH];
        private final ByteArrayHasher hasher;

        private ConsistentLongHasher(final int seed) {
            state[0] = -1; // any value that has len > 1
            hasher = new ByteArrayHasher(seed);
        }

        public int calculateHash(final long value) {
            final int oldLen = curValueLen;
            curValueLen = getStrLen(value);
            // clearing data if new value is shorter that old.
            for (int i = curValueLen; i < oldLen; i++) {
                curValue[i] = 0;
                state[i] = 0;
            }

            // calculating string representation digit-by-digit
            // and check for common prefix.
            long v = value;
            int pos = curValueLen - 1;
            while (true) {
                if (state[pos] == v) {
                    // common prefix found.
                    return hasher.calculateHash(curValue, curValueLen, pos + 1);
                }
                curValue[pos] = (byte) (ZERO + Math.abs(v % 10));
                state[pos] = v;
                v /= 10;
                if (v == 0) {
                    if (value < 0) {
                        // don't forget minus for negative.
                        pos--;
                        curValue[pos] = MINUS;
                        state[pos] = -1; // any value that has len > 1
                    }
                    if (pos != 0) {
                        // if we fully converted value to string we must be in the beginning.
                        throw new IllegalStateException("internal error");
                    }
                    // no common prefix, recalculate all.
                    return hasher.calculateHash(curValue, curValueLen, 0);
                }
                pos--;
            }
        }
    }

    // Simple helper class over ByteArrayHasher
    public final class StringHasher {
        private final ByteArrayHasher hasher;

        private StringHasher(final int seed) {
            hasher = new ByteArrayHasher(seed);
        }

        public int calculateHash(final String value) {
            final byte[] bytes = value.getBytes(Charsets.UTF_8);
            return hasher.calculateHash(bytes, bytes.length);
        }
    }

    // Hasher for byte[]
    public final class ByteArrayHasher {
        private final int seed;
        private int[] cachedResult = new int[0];
        private int[] cachedValue = new int[0];

        public ByteArrayHasher(final int seed) {
            this.seed = seed;
        }

        public int calculateHash(final byte[] value, final int valueLen) {
            return calculateHash(value, valueLen, 0);
        }

        public int calculateHash(final byte[] value, final int valueLen, final int prefixLen) {
            final int newResultLen = (valueLen + 3) / 4;
            if (newResultLen > cachedResult.length) {
                cachedResult = Arrays.copyOf(cachedResult, newResultLen);
                cachedValue = Arrays.copyOf(cachedValue, newResultLen);
            }
            for (int i = newResultLen + 1; i < cachedResult.length; i++) {
                // We can do it since our data is from string and has no zero bytes.
                // So we never match with zero value in cache.
                // Otherwise we need to keep last data len to track what part of cached data is valid.
                cachedResult[i] = 0;
                cachedValue[i] = 0;
            }
            // pos is first index of 4-byte chunk
            int pos = (prefixLen / 4) * 4;
            int prevHash = (pos == 0) ? seed : cachedResult[(pos/4)-1];
            // isSame became false after first cache miss.
            boolean isSame = true;

            // processing 4-byte chunks
            while(pos <= (valueLen - 4))  {
                final int curValue = value[pos]
                        | (value[pos+1] << 8)
                        | (value[pos+2] << 16)
                        | (value[pos+3] << 24);
                if (isSame && (curValue == cachedValue[pos/4])) {
                    prevHash = cachedResult[pos/4];
                } else {
                    isSame = false;
                    prevHash = hashStep(curValue, prevHash);
                    cachedResult[pos/4] = prevHash;
                }
                pos += 4;
            }

            // processing tail
            int lastValue = 0;
            switch (valueLen & 3) {
                case 3:
                    lastValue |= (value[pos+2] << 16);
                    // fall through
                case 2:
                    lastValue |= (value[pos+1] << 8);
                    // fall through
                case 1:
                    lastValue |= value[pos];
                    if (isSame && (lastValue == cachedValue[pos/4])) {
                        prevHash = cachedResult[pos/4];
                    } else {
                        prevHash = hashStep(lastValue, prevHash);
                        cachedResult[pos/4] = prevHash;
                    }
                    // fall through
                case 0:
                    return prevHash;
            }

            throw new IllegalStateException("unreachable code");
        }
    }

    // Hasher implementation based on murmur3 hash function.
    public static class Murmur3Hasher extends IterativeHasher {

        public Murmur3Hasher(final String salt) {
            super(salt);
        }

        /*
        A sample C implementation (for little-endian CPUs)
        From https://en.wikipedia.org/wiki/MurmurHash

        uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed) {
            uint32_t h = seed;
            if (len > 3) {
                const uint32_t* key_x4 = (const uint32_t*) key;
                size_t i = len >> 2;
                do {
                    uint32_t k = *key_x4++;
                    k *= 0xcc9e2d51;
                    k = (k << 15) | (k >> 17);
                    k *= 0x1b873593;
                    h ^= k;
                    h = (h << 13) | (h >> 19);
                    h = (h * 5) + 0xe6546b64;
                } while (--i);
                key = (const uint8_t*) key_x4;
            }
            if (len & 3) {
                size_t i = len & 3;
                uint32_t k = 0;
                key = &key[i - 1];
                do {
                    k <<= 8;
                    k |= *key--;
                } while (--i);
                k *= 0xcc9e2d51;
                k = (k << 15) | (k >> 17);
                k *= 0x1b873593;
                h ^= k;
            }
            h ^= len;
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }

        hashStep(value, seed) is equivalent to murmur3_32((uint8_t*)&value, 4, seed)
        after substitution and simplification olny one iteration inside do-while cycle left
        and second if is deleted since len==4
        all right shifts are logical
        */

        @Override
        public int hashStep(final int value, final int prevHash) {
            int h = prevHash;
            int k = value;
            k *= 0xcc9e2d51;
            k = (k << 15) | (k >>> 17);
            k *= 0x1b873593;
            h ^= k;
            h = (h << 13) | (h >>> 19);
            h = (h * 5) + 0xe6546b64;
            h ^= 4; // len == 4
            h ^= h >>> 16;
            h *= 0x85ebca6b;
            h ^= h >>> 13;
            h *= 0xc2b2ae35;
            h ^= h >>> 16;
            return h;
        }
    }
}

