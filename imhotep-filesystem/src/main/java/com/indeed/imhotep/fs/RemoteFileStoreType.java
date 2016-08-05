package com.indeed.imhotep.fs;

/**
 * @author kenh
 */

public enum RemoteFileStoreType {
    LOCAL("local", new LocalFileStore.Builder()),
    S3("s3", new S3RemoteFileStore.Builder());

    private final String name;
    private final RemoteFileStore.Builder builder;

    RemoteFileStoreType(final String name, final RemoteFileStore.Builder builder) {
        this.name = name;
        this.builder = builder;
    }

    public String getName() {
        return name;
    }

    public RemoteFileStore.Builder getBuilder() {
        return builder;
    }

    public static RemoteFileStoreType fromName(final String name) {
        for (final RemoteFileStoreType type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unsupported type name: " + name);
    }
}
