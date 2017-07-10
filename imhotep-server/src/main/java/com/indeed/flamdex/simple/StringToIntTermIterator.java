package com.indeed.flamdex.simple;

import com.google.common.base.Supplier;
import com.indeed.flamdex.reader.GenericStringToIntTermIterator;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Allows iteration over string Flamdex fields in numeric order.
 *
 * @author vladimir
 */

public class StringToIntTermIterator extends GenericStringToIntTermIterator<SimpleStringTermIterator> implements SimpleIntTermIterator {
    private final Path filename;

    /**
     * @param stringTermIterator         initial iterator used for initialization
     * @param stringTermIteratorSupplier used to create new SimpleStringTermIterator instances to have multiple iteration cursors in parallel
     */
    public StringToIntTermIterator(SimpleStringTermIterator stringTermIterator, Supplier<SimpleStringTermIterator> stringTermIteratorSupplier) {
        super(stringTermIterator, stringTermIteratorSupplier);
        this.filename = stringTermIterator.getFilename();
    }

    @Override
    public Path getFilename() {
        return filename;
    }

    @Override
    public long getOffset() {
        sanityCheck();
        return getCurrentStringTermIterator().getOffset();
    }

    @Override
    public long getDocListAddress() throws IOException {
        return stringTermIterator.getDocListAddress();
    }
}
