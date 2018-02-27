package com.indeed.imhotep.group;

public class TestMurmur3Hasher extends TestIterativeHasherBase {

    @Override
    public IterativeHasher createHasher(final String salt) {
        return new IterativeHasher.Murmur3Hasher(salt);
    }
}
