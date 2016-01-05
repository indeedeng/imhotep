package com.indeed.imhotep.local;

import java.util.Random;

enum MetricMaxSizes {
    LONG(Long.MAX_VALUE, Long.MIN_VALUE, "long") {
        @Override
        public long randVal() {
            return this.foo.nextLong();
        }
    },
    INT(Integer.MAX_VALUE, Integer.MIN_VALUE, "int"){
        @Override
        public long randVal() {
            return this.foo.nextInt();
        }
    },
    CHAR(65535, 0, "unsigned short"){
        @Override
        public long randVal() {
            return (char)this.foo.nextInt();
        }
    },
    SHORT(Short.MAX_VALUE, Short.MIN_VALUE, "short") {
        @Override
        public long randVal() {
            return (short)this.foo.nextInt();
        }
    },
    SIGNED_BYTE(Byte.MAX_VALUE, Byte.MIN_VALUE, "signed byte") {
        @Override
        public long randVal() {
            byte[] b = new byte[1];

            this.foo.nextBytes(b);
            return b[0];
        }
    },
    BYTE(255,0,"unsigned byte") {
        @Override
        public long randVal() {
            byte[] b = new byte[1];

            this.foo.nextBytes(b);
            return b[0] - Byte.MIN_VALUE;
        }
    },
    BINARY(1, 0, "binary") {
        @Override
        public long randVal() {
            if (this.foo.nextBoolean())
                return 1;
            else
                return 0;
        }
    };

    protected final long maxVal;
    protected final long minVal;
    protected final String name;
//        protected final NotRandom foo = NotRandom.closed(0, 1 << 20);
    protected final Random foo = new Random();

    MetricMaxSizes(long maxVal, long minVal, String name) {
        this.maxVal = maxVal;
        this.minVal = minVal;
        this.name = name;
    }

    MetricMaxSizes(String name) {
        this(Integer.MAX_VALUE, 0, name);
    }

    public long getMaxVal() {
        return maxVal;
    }

    public long getMinVal() {
        return minVal;
    }

    public String getName() {
        return name;
    }

    public abstract long randVal();

    public static MetricMaxSizes getRandomSize() {
        switch (LONG.foo.nextInt(7)) {
            case 0:
                return LONG;
            case 1:
                return INT;
            case 2:
                return CHAR;
            case 3:
                return SHORT;
            case 4:
                return SIGNED_BYTE;
            case 5:
                return BYTE;
            case 6:
                return BINARY;
            default:
                throw new UnsupportedOperationException("Wha?!");
        }
    }
}
