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

package com.indeed.imhotep;

import com.indeed.imhotep.FTGSBinaryFormat.FieldStat;
import com.indeed.util.core.Pair;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * @author kenh
 */

public class InputStreamFTGSIterators {
    private InputStreamFTGSIterators() {}

    private static InputStream createInputStream(final File file) throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(file));
    }

    public static InputStreamFTGSIterator create(final File file,
                                                 final FieldStat[] fieldStats,
                                                 final int numStats,
                                                 final int numGroups) throws FileNotFoundException {
        return new InputStreamFTGSIterator(createInputStream(file), fieldStats, numStats, numGroups);
    }

    public static InputStreamFTGSIterator create(final Pair<File, FieldStat[]> fileAndStats,
                                                 final int numStats,
                                                 final int numGroups) throws FileNotFoundException {
        return create(fileAndStats.getFirst(), fileAndStats.getSecond(), numStats, numGroups);
    }
}
