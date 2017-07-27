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

import java.nio.file.Path;

/**
 * @author jsgroth
 */
final class NullIntTermIterator implements SimpleIntTermIterator {
    private final Path docsFilename;

    NullIntTermIterator(final Path docsFilename) {
        this.docsFilename = docsFilename;
    }

    @Override
    public void reset(final long term) {
    }

    @Override
    public long term() {
        return 0;
    }

    @Override
    public Path getFilename() {
        return docsFilename;
    }

    @Override
    public long getOffset() {
        return 0L;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public int docFreq() {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public long getDocListAddress() {
        return 0;
    }
}
