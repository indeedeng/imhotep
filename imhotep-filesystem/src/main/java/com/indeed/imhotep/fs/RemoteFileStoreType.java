package com.indeed.imhotep.fs;

/**
 * @author kenh
 */

public enum RemoteFileStoreType {
    LOCAL("local", new LocalFileStore.Factory()),
    S3("s3", new S3RemoteFileStore.Factory()),
    HDFS("hdfs", new HdfsRemoteFileStore.Factory());

    private final String name;
    private final RemoteFileStore.Factory factory;

    RemoteFileStoreType(final String name, final RemoteFileStore.Factory factory) {
        this.name = name;
        this.factory = factory;
    }

    public String getName() {
        return name;
    }

    public RemoteFileStore.Factory getFactory() {
        return factory;
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
