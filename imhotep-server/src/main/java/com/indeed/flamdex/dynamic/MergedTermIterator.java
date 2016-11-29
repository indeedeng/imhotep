package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.TermIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nonnull;

/**
 * @author michihiko
 */
abstract class MergedTermIterator implements TermIterator {
    @Nonnull
    abstract TermIterator getInnerTermIterator(final int idx);
    @Nonnull
    abstract IntList getCurrentMinimums();
}
