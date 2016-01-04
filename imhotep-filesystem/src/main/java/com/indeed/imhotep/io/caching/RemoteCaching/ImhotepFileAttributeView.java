package com.indeed.imhotep.io.caching.RemoteCaching;

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
