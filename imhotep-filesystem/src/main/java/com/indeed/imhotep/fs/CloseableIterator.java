package com.indeed.imhotep.fs;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Created by darren on 4/7/16.
 */
interface CloseableIterator<T> extends Iterator<T>, Closeable { }
