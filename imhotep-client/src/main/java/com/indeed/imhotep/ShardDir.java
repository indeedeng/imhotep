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
package com.indeed.imhotep;

import com.google.common.base.Objects;
import com.sun.istack.NotNull;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShardDir {
    private static final Pattern VERSION_PATTERN = Pattern.compile("^(index.+)\\.(\\d{14})$");

    private final String name;
    private final Path indexDir;
    private final String id;
    private final long version;
    private final String dataset;
    private org.apache.hadoop.fs.Path hadoopPath;

    public ShardDir(final Path path) {
        this.dataset = path.getParent().getFileName().toString();
        this.name = path.getFileName().toString();
        this.indexDir = path;

        final Matcher matcher = VERSION_PATTERN.matcher(name);
        if (matcher.matches()) {
            this.id = matcher.group(1);
            this.version = Long.parseLong(matcher.group(2));
        } else if (DynamicIndexSubshardDirnameUtil.isValidDynamicIndexName(this.name)) {
            final DynamicIndexSubshardDirnameUtil.DynamicIndexShardInfo dynamicIndexShardInfo = DynamicIndexSubshardDirnameUtil.parse(this.name);
            this.id = dynamicIndexShardInfo.getId();
            this.version = dynamicIndexShardInfo.getUpdateId();
        } else {
            this.id = name;
            this.version = 0L;
        }
    }

    public ShardDir(org.apache.hadoop.fs.Path path) {
        this(cleanPath(path));
        hadoopPath = path;
    }

    private static Path cleanPath(org.apache.hadoop.fs.Path path) {
        final String endName = path.getName();
        final int endIndex = endName.lastIndexOf(".sqar");
        if(endIndex>-1) {
            final String actualEndName = endName.substring(0, endIndex);
            return Paths.get(path.getParent().getName(), actualEndName);
        }
        return Paths.get(path.getParent().getName(), endName);
    }

    public static Path cleanPath(final Path path) {
        final String endName = path.getFileName().toString();
        final int endIndex = endName.lastIndexOf(".sqar");
        if (endIndex > -1) {
            final String actualEndName = endName.substring(0, endIndex);
            return path.getParent().resolve(actualEndName);
        }
        return path;
    }

    @NotNull
    public org.apache.hadoop.fs.Path getHadoopPath(){
        if(hadoopPath == null){
            throw new UnsupportedOperationException("This shard was not instantiated with a hadoop path.");
        }
        return hadoopPath;
    }

    public String getDataset() {return dataset;}

    public String getId() {
        return id;
    }

    @Nullable
    public Interval getTimeInterval() {
        final ShardInfo.DateTimeRange range = ShardInfo.parseDateTime(id);
        if (range == null) {
            return null;
        }
        return new Interval(range.start, range.end);
    }

    public long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public Path getIndexDir() {
        return indexDir;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShardDir)) {
            return false;
        }
        final ShardDir shardDir = (ShardDir) o;
        return (version == shardDir.version) &&
                Objects.equal(name, shardDir.name) &&
                Objects.equal(indexDir, shardDir.indexDir) &&
                Objects.equal(id, shardDir.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, indexDir, id, version);
    }

    @Override
    public String toString() {
        return "ShardDir{" +
                "name='" + name + '\'' +
                ", indexDir=" + indexDir +
                ", id='" + id + '\'' +
                ", version=" + version +
                '}';
    }
}
