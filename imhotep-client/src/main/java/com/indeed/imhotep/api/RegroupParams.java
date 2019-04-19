package com.indeed.imhotep.api;

import com.google.common.base.Objects;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
public class RegroupParams {
    public static final RegroupParams DEFAULT = new RegroupParams(ImhotepSession.DEFAULT_GROUPS, ImhotepSession.DEFAULT_GROUPS);

    private final String inputGroups;
    private final String outputGroups;

    public static RegroupParams fromImhotepRequest(final ImhotepRequest imhotepRequest) {
        return new RegroupParams(
                imhotepRequest.getInputGroups(),
                imhotepRequest.getOutputGroups()
        );
    }
}
