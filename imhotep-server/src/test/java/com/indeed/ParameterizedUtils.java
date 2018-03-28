package com.indeed;

import com.indeed.flamdex.simple.SimpleFlamdexReader;

import java.util.ArrayList;
import java.util.List;

// Helper class for creating various input parameters for @RunWith(Parameterized.class) tests
public final class ParameterizedUtils {
    private ParameterizedUtils() {
    }

    public static Iterable<SimpleFlamdexReader.Config[]> getAllPossibleFlamdexConfigs() {
        final List<SimpleFlamdexReader.Config[]> result = new ArrayList<>(64);
        for (int variant = 0; variant < 64; variant++) {
            final SimpleFlamdexReader.Config config = new SimpleFlamdexReader.Config()
                    .setWriteBTreesIfNotExisting((variant&32)!=0)
                    .setWriteCardinalityIfNotExisting((variant&16)!=0)
                    .setUseMMapMetrics((variant&8)!=0)
                    .setUseMMapDocIdStream((variant&4)!=0)
                    .setUseNativeDocIdStream((variant&2)!=0)
                    .setUseSSSE3((variant&1)!=0);
            result.add(new SimpleFlamdexReader.Config[] {config});
        }
        return result;
    }
}