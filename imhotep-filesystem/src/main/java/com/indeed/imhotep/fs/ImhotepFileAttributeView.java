package com.indeed.imhotep.fs;

import java.nio.file.attribute.FileAttributeView;

/**
 * @author darren
 */
class ImhotepFileAttributeView implements FileAttributeView {
    @Override
    public String name() {
        return "imhotep";
    }

}
