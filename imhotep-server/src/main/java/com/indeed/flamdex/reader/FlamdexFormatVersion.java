package com.indeed.flamdex.reader;

/**
 * @author michihiko
 */
public enum FlamdexFormatVersion {
    SIMPLE(0),
    @Deprecated PFORDELTA(1),
    LUCENE(2),
    DYNAMIC(3);

    private final int version;

    FlamdexFormatVersion(final int version) {
        this.version = version;
    }

    public int getVersionNumber() {
        return version;
    }

    public static FlamdexFormatVersion fromVersionNumber(final int versionNumber) {
        for (final FlamdexFormatVersion flamdexFormatVersion : FlamdexFormatVersion.values()) {
            if (versionNumber == flamdexFormatVersion.version) {
                return flamdexFormatVersion;
            }
        }
        throw new IllegalArgumentException("index format version " + versionNumber + " not supported");
    }
}
