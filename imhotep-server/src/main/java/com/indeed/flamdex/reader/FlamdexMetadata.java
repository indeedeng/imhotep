/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.flamdex.reader;

import com.google.common.base.Charsets;

import org.yaml.snakeyaml.JavaBeanDumper;
import org.yaml.snakeyaml.JavaBeanLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
* @author jplaisance
*/
public class FlamdexMetadata {
    public int numDocs;
    public List<String> intFields;
    public List<String> stringFields;
    public int formatVersion;

    public FlamdexMetadata() {
    }

    public FlamdexMetadata(int numDocs, List<String> intFields, List<String> stringFields, int formatVersion) {
        this.numDocs = numDocs;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.formatVersion = formatVersion;
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

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(final int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public static FlamdexMetadata readMetadata(final Path directory) throws IOException {
        JavaBeanLoader<FlamdexMetadata> loader = new JavaBeanLoader<FlamdexMetadata>(FlamdexMetadata.class);
        final Path metadataPath = directory.resolve("metadata.txt");

        try (final BufferedReader metadataReader = Files.newBufferedReader(metadataPath, Charsets.UTF_8)) {
            final FlamdexMetadata results = loader.load(metadataReader);
            return results;
        }
    }

    public static void writeMetadata(final Path directory, FlamdexMetadata metadata) throws IOException {
        JavaBeanDumper dumper = new JavaBeanDumper(false);

        Files.write(directory.resolve("metadata.txt"),
                    dumper.dump(metadata).getBytes(Charsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
    }
}
