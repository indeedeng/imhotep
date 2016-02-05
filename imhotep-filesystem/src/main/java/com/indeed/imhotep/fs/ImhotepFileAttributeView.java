package com.indeed.imhotep.fs;

import java.nio.file.attribute.FileAttributeView;

/**
 * Created by darren on 12/22/15.
 */
public class ImhotepFileAttributeView implements FileAttributeView {
    @Override
    public String name() {
        return "basic";
    }

}
