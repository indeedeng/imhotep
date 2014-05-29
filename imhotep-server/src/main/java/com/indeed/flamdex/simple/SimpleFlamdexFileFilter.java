package com.indeed.flamdex.simple;

import java.io.File;
import java.io.FileFilter;

/**
 * @author jsgroth
 */
public class SimpleFlamdexFileFilter implements FileFilter {
    @Override
    public boolean accept(File pathname) {
        final String name = pathname.getName();
        if ("metadata.txt".equals(name)) return true;
        if (name.startsWith("fld-")) {
            if (name.endsWith(".intterms")) return true;
            if (name.endsWith(".strterms")) return true;
            if (name.endsWith(".intdocs")) return true;
            if (name.endsWith(".strdocs")) return true;
            if (name.endsWith(".intindex") && pathname.isDirectory()) return true;
            if (name.endsWith(".intindex64") && pathname.isDirectory()) return true;
            if (name.endsWith(".strindex") && pathname.isDirectory()) return true;
        }
        return false;
    }
}
