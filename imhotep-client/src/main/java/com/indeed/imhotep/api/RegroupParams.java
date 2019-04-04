package com.indeed.imhotep.api;

import com.google.common.base.Objects;
import com.indeed.imhotep.protobuf.ImhotepRequest;

public class RegroupParams {
    public static final RegroupParams DEFAULT = new RegroupParams(ImhotepSession.DEFAULT_GROUPS, ImhotepSession.DEFAULT_GROUPS);

    private final String inputGroups;
    private final String outputGroups;

    public RegroupParams(final String inputGroups, final String outputGroups) {
        this.inputGroups = inputGroups;
        this.outputGroups = outputGroups;
    }

    public static RegroupParams fromImhotepRequest(final ImhotepRequest imhotepRequest) {
        return new RegroupParams(
                imhotepRequest.getInputGroups(),
                imhotepRequest.getOutputGroups()
        );
    }

    public String getInputGroups() {
        return inputGroups;
    }

    public String getOutputGroups() {
        return outputGroups;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RegroupParams that = (RegroupParams) o;
        return Objects.equal(inputGroups, that.inputGroups) &&
                Objects.equal(outputGroups, that.outputGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputGroups, outputGroups);
    }

    @Override
    public String toString() {
        return "RegroupParams{" +
                "inputGroup='" + inputGroups + '\'' +
                ", outputGroup='" + outputGroups + '\'' +
                '}';
    }
}
