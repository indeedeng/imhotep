package com.indeed.flamdex.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kornerup
 */

public class ShardMetadataUtils {

    public static class AllFields {
        final public List<String> strFields;
        final public List<String> intFields;
        public AllFields() {
            strFields = new ArrayList<>();
            intFields = new ArrayList<>();
        }
    }

    public static AllFields getFieldsFromFlamdexFiles(final List<Path> paths) throws IOException {
        AllFields fields = new AllFields();
        for (final Path file : paths) {
            final String name = file.getFileName().toString();
            if (name.startsWith("fld-") && name.endsWith(".intterms")) {
                fields.intFields.add(name.substring(4, name.length() - 9));
            } else if (name.startsWith("fld-") && name.endsWith(".strterms")) {
                fields.strFields.add(name.substring(4, name.length() - 9));
            }
        }

        return fields;
    }
}
