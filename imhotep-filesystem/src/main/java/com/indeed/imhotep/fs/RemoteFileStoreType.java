/*
 * Copyright (C) 2018 Indeed Inc.
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
