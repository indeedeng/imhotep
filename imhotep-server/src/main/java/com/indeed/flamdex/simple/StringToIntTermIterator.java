/*
 * Copyright (C) 2018 Indeed Inc.
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
    public StringToIntTermIterator(
            final SimpleStringTermIterator stringTermIterator,
            final Supplier<SimpleStringTermIterator> stringTermIteratorSupplier) {
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
