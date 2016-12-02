package com.indeed.flamdex.dynamic;

import com.indeed.flamdex.api.TermIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nonnull;

/**
 * @author michihiko
 */
interface MergedTermIterator extends TermIterator {
    @Nonnull
    TermIterator getInnerTermIterator(final int idx);

    @Nonnull
    IntList getCurrentMinimums();
}
