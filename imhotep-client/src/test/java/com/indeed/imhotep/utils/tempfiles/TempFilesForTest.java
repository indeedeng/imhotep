package com.indeed.imhotep.utils.tempfiles;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class TempFilesForTest extends AbstractTempFiles<TempFilesForTest.Type> {
    private TempFilesForTest(final Builder<Type, TempFilesForTest> builder) {
        super(builder);
    }

    static Builder<Type, TempFilesForTest> builder() {
        return new Builder<>(Type.class, TempFilesForTest::new);
    }

    @Getter
    @AllArgsConstructor
    enum Type implements TempFileType<Type> {
        A("a"),
        B("b");
        final String identifier;
    }
}
