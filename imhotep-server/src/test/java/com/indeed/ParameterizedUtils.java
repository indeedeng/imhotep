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

package com.indeed;

import com.indeed.flamdex.simple.SimpleFlamdexReader;

import java.util.ArrayList;
import java.util.List;

// Helper class for creating various input parameters for @RunWith(Parameterized.class) tests
public final class ParameterizedUtils {
    private ParameterizedUtils() {
    }

    public static Iterable<SimpleFlamdexReader.Config[]> getFlamdexConfigs() {
        final List<SimpleFlamdexReader.Config> tests = new ArrayList<>();

        // test field cacher without mmap, ByteChannelDocIdStream, Generic*TermDocIterator
        tests.add(empty());
        // test BTree writing
        tests.add(empty().setWriteBTreesIfNotExisting(true));
        // test cardinality writing
        tests.add(empty().setWriteCardinalityIfNotExisting(true));
        // test field cacher with mmap
        tests.add(empty().setUseMMapMetrics(true));
        // test MMapDocIdStream class
        tests.add(empty().setUseMMapDocIdStream(true));
        // test Native*TermDocIteraror without SSSE3, NativeDocIdStream without SSSE3,
        // NativeDocIdStream without SSSE3
        tests.add(empty().setUseMMapDocIdStream(true).setUseNativeDocIdStream(true));
        // test Native*TermDocIteraror with SSSE3, NativeDocIdStream with SSSE3,
        // NativeDocIdStream with SSSE3
        tests.add(empty().setUseMMapDocIdStream(true).setUseNativeDocIdStream(true).setUseSSSE3(true));
        // test default config
        // If default config is equal with something above, we just test it one more time.
        tests.add(new SimpleFlamdexReader.Config());

        final List<SimpleFlamdexReader.Config[]> result = new ArrayList<>();
        for (final SimpleFlamdexReader.Config test : tests) {
            result.add(new SimpleFlamdexReader.Config[] {test});
        }
        return result;
    }

    private static SimpleFlamdexReader.Config empty() {
        return new SimpleFlamdexReader.Config()
                .setWriteBTreesIfNotExisting(false)
                .setWriteCardinalityIfNotExisting(false)
                .setUseMMapMetrics(false)
                .setUseMMapDocIdStream(false)
                .setUseNativeDocIdStream(false)
                .setUseSSSE3(false);
    }
}