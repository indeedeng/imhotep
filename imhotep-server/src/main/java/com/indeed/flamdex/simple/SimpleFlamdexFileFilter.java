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
 package com.indeed.flamdex.simple;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author jsgroth
 */
public class SimpleFlamdexFileFilter implements DirectoryStream.Filter<Path> {
    @Override
    public boolean accept(final Path entry) throws IOException {
        final String name = entry.getFileName().toString();
        final boolean isDirectory = Files.isDirectory(entry);
        if ("metadata.txt".equals(name)) return true;
        if (name.startsWith("fld-")) {
            if (name.endsWith(".intterms")) return true;
            if (name.endsWith(".strterms")) return true;
            if (name.endsWith(".intdocs")) return true;
            if (name.endsWith(".strdocs")) return true;
            if (name.endsWith(".intindex") && isDirectory) return true;
            if (name.endsWith(".intindex64") && isDirectory) return true;
            if (name.endsWith(".strindex") && isDirectory) return true;
        }
        return false;
    }

}
