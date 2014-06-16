package com.indeed.flamdex.reader;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.indeed.imhotep.io.caching.CachedFile;

import org.yaml.snakeyaml.JavaBeanDumper;
import org.yaml.snakeyaml.JavaBeanLoader;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    public static FlamdexMetadata readMetadata(final String directory) throws IOException {
        JavaBeanLoader<FlamdexMetadata> loader = new JavaBeanLoader<FlamdexMetadata>(FlamdexMetadata.class);
        File metadataFile;
        metadataFile = CachedFile.create(CachedFile.buildPath(directory, "metadata.txt")).loadFile();
        String metadata = Files.toString(metadataFile, Charsets.UTF_8);
        return loader.load(metadata);
    }

    public static void writeMetadata(final String directory, FlamdexMetadata metadata) throws IOException {
        JavaBeanDumper dumper = new JavaBeanDumper(false);
        Files.write(dumper.dump(metadata).getBytes(Charsets.UTF_8), new File(directory, "metadata.txt"));
    }
}
