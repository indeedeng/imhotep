package com.indeed.imhotep.utils.tempfiles;

import lombok.Value;

import javax.annotation.Nullable;
import java.nio.file.Path;

@Value
public class TempFileState {
    final TempFileType tempFileType;
    final Path path;
    @Nullable final StackTraceElement[] stackTraceElements;
}
