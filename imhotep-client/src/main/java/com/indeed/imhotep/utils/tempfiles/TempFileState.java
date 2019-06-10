package com.indeed.imhotep.utils.tempfiles;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.ToString;
import lombok.Value;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Value
public class TempFileState {
    final TempFileType tempFileType;
    final Path path;
    final long creationTimestamp;
    final int refCount;
    final boolean removed;
    @JsonIgnore
    @ToString.Exclude
    @Nullable final StackTraceElement[] stackTraceElements;

    @JsonGetter
    @Nullable
    List<String> getStackTrace() {
        if (stackTraceElements == null) {
            return null;
        }
        return Arrays.stream(stackTraceElements)
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
    }

    @JsonGetter
    @ToString.Include
    String durationSinceCreation() {
        return Duration.of(System.currentTimeMillis() - creationTimestamp, ChronoUnit.MILLIS).toString();
    }
}
