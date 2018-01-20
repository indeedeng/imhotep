package com.indeed.flamdex.api;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * author: aibragimov
 *
 */
public class FieldsCardinalityMetadata {

    private final Map<String, FieldInfo> fieldsInfo;

    // Current solution is to store metadata in a separate file in flamdex directory.
    // This will likely to change in future: field cardinaliry will be in metadata.txt
    private static final String FILENAME = "metadata.cardinality";

    // Info about docs-to-terms relation for a field.
    public static class FieldInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        // TRUE => there is at least one doc with no terms
        // FALSE => all docs have one or more terms
        // null => unknown
        public final Boolean hasZeroTermDoc;

        // TRUE => there is at least one doc with has exactly one term
        // FALSE => all docs have zero terms or more than one term
        // null => unknown
        public final Boolean hasSingleTermDoc;

        // TRUE => there is at least one doc which has exactly one term
        // FALSE => all docs have zero terms or more than one term
        // null => unknown
        public final Boolean hasMultipleTermDoc;

        public FieldInfo(final Boolean zero, final Boolean single, final Boolean multiple) {
            hasZeroTermDoc = zero;
            hasSingleTermDoc = single;
            hasMultipleTermDoc = multiple;
        }
    }

    @Nullable
    public FieldInfo intFieldInfo(final String intField) {
        return fieldInfo(intField, true);
    }

    @Nullable
    public FieldInfo stringFieldInfo(final String stringField) {
        return fieldInfo(stringField, false);
    }

    private static String keyName(final String fieldName, final boolean isIntField) {
        return (isIntField ? "intField:" : "stringField:") + fieldName;
    }

    private FieldInfo fieldInfo(final String field, final boolean isIntField) {
        return fieldsInfo.get(keyName(field,isIntField));
    }

    public static boolean hasMetadataFile(final Path dir) {
        return !Files.notExists(getMetadataFile(dir));
    }

    private static Path getMetadataFile(final Path dir) {
        return dir.resolve(FILENAME);
    }

    public void writeToDirectory(final Path directory) throws IOException {
        try(FileOutputStream fileOut = new FileOutputStream(getMetadataFile(directory).toFile());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(fieldsInfo);
        }
    }

    private FieldsCardinalityMetadata(final Map<String, FieldInfo> fieldsInfo) {
        this.fieldsInfo = fieldsInfo;
    }

    public static FieldsCardinalityMetadata open(final Path directory) {
        final Path metadataFile = getMetadataFile(directory);

        if (Files.notExists(metadataFile)) {
            return null;
        }

        try(FileInputStream fileIn = new FileInputStream(metadataFile.toFile());
                ObjectInputStream in = new ObjectInputStream(fileIn)) {
            final Object obj = in.readObject();
            final Map<String, FieldInfo> fieldsInfo = (Map<String, FieldInfo>) obj;
            return new FieldsCardinalityMetadata(fieldsInfo);
        } catch (final IOException | ClassNotFoundException ex) {
            // in case of error return empty metadata
            return new FieldsCardinalityMetadata(new HashMap<>());
        }
    }

    public static class Builder {
        private final Map<String, FieldInfo> fieldsInfo = new HashMap<>();

        public Builder addIntField(final String fieldName, final boolean zero, final boolean single, final boolean multiple) {
            fieldsInfo.put(keyName(fieldName, true), new FieldInfo(zero, single, multiple));
            return this;
        }

        public Builder addIntField(final String fieldName, final FieldInfo info) {
            fieldsInfo.put(keyName(fieldName, true), info);
            return this;
        }

        public Builder addStringField(final String fieldName, final boolean zero, final boolean single, final boolean multiple) {
            fieldsInfo.put(keyName(fieldName, false), new FieldInfo(zero, single, multiple));
            return this;
        }

        public Builder addStringField(final String fieldName, final FieldInfo info) {
            fieldsInfo.put(keyName(fieldName, false), info);
            return this;
        }

        public FieldsCardinalityMetadata build() {
            return new FieldsCardinalityMetadata(fieldsInfo);
        }
    }
}
