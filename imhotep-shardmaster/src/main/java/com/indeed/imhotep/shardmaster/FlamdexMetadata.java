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
package com.indeed.imhotep.shardmaster;

import com.google.common.base.Charsets;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.SquallArchiveReader;
import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * @author jplaisance
 */
public class FlamdexMetadata {
    private int numDocs;
    private List<String> intFields;
    private List<String> stringFields;
    private FlamdexFormatVersion flamdexFormatVersion;

    public FlamdexMetadata() {
    }

    public FlamdexMetadata(final int numDocs, final List<String> intFields, final List<String> stringFields, final FlamdexFormatVersion flamdexFormatVersion) {
        this.numDocs = numDocs;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.flamdexFormatVersion = flamdexFormatVersion;
    }

    /**
     * Please use enum version above.
     */
    @Deprecated
    public FlamdexMetadata(final int numDocs, final List<String> intFields, final List<String> stringFields, final int formatVersion) {
        this(numDocs, intFields, stringFields, FlamdexFormatVersion.fromVersionNumber(formatVersion));
    }

    public int getNumDocs() {
        return numDocs;
    }

    public void setNumDocs(final int numDocs) {
        this.numDocs = numDocs;
    }

    public List<String> getIntFields() {
        return intFields;
    }

    public void setIntFields(final List<String> intFields) {
        this.intFields = intFields;
    }

    public List<String> getStringFields() {
        return stringFields;
    }

    public void setStringFields(final List<String> stringFields) {
        this.stringFields = stringFields;
    }

    /**
     * This is for snakeyaml.
     * Please consider using getFlamdexFormatVersion instead.
     */
    @Deprecated
    public int getFormatVersion() {
        return flamdexFormatVersion.getVersionNumber();
    }

    @Deprecated
    public void setFormatVersion(final int formatVersion) {
        flamdexFormatVersion = FlamdexFormatVersion.fromVersionNumber(formatVersion);
    }

    /**
     * To prevent YAML format change, please don't add setFlamdexFormatVersion().
     */
    public FlamdexFormatVersion getFlamdexFormatVersion() {
        return flamdexFormatVersion;
    }

    public static FlamdexMetadata readMetadata(final Path directory) throws IOException {
        // Pass the class loader to the constructor, to avoid getting a "class FlamdexMetadata not found exception".
        // The exception happens because, on the workers, the YAML parser does not know how to create an instance of
        // FlamdexMetadata because of version mismatch caused by the presence of many jars.
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path("/var/imhotep"+directory.toString()+".sqar");
        org.apache.hadoop.fs.FileSystem hadoopFilesystem = new org.apache.hadoop.fs.Path("/var/imhotep").getFileSystem(new Configuration());
        SquallArchiveReader reader = new SquallArchiveReader(hadoopFilesystem, hadoopPath);
        List<FileMetadata> fileMetadata = reader.readMetadata();
        FileMetadata acutalMetadata = getMetadataFromMetadata(fileMetadata);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        reader.tryCopyToStream(acutalMetadata, out);
        final Yaml loader = new Yaml(new CustomClassLoaderConstructor(FlamdexMetadata.class, FlamdexMetadata.class.getClassLoader()));

        final String metadata = out.toByteArray().toString();
        return loader.loadAs(metadata, FlamdexMetadata.class);
    }

    private static FileMetadata getMetadataFromMetadata(List<FileMetadata> fileMetadata) {
        for(FileMetadata data: fileMetadata){
            if(data.getFilename().equals("metadata.txt")){
                return data;
            }
        }
        return null;
    }

    public static void writeMetadata(final Path directory, final FlamdexMetadata metadata) throws IOException {
        final Yaml dumper = new Yaml();
        final String s = dumper.dumpAs(metadata, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
        Files.write(directory.resolve("metadata.txt"), s.getBytes(Charsets.UTF_8));
    }

    @Override
    public String toString() {
        return "FlamdexMetadata{" +
                "numDocs=" + numDocs +
                ", intFields=" + intFields +
                ", stringFields=" + stringFields +
                '}';
    }
}
