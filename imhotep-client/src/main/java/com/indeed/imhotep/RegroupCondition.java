package com.indeed.imhotep;

public class RegroupCondition {
    public final String field;
    public final boolean intType;
    public final long intTerm;
    public final String stringTerm;
    public final boolean inequality;

    public RegroupCondition(String field, boolean intType, long intTerm, String stringTerm, boolean inequality) {
        this.field = field.intern();
        this.intType = intType;
        this.intTerm = intTerm;
        this.stringTerm = stringTerm;
        this.inequality = inequality;
    }

    public String toString() {
        return field+(inequality?" <= ":" = ")+(intType?intTerm:stringTerm);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegroupCondition that = (RegroupCondition) o;

        if (inequality != that.inequality) return false;
        if (intTerm != that.intTerm) return false;
        if (intType != that.intType) return false;
        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (stringTerm != null ? !stringTerm.equals(that.stringTerm) : that.stringTerm != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (intType ? 1 : 0);
        result = 31 * result + (int)intTerm;
        result = 31 * result + (int)(intTerm>>>32);
        result = 31 * result + (stringTerm != null ? stringTerm.hashCode() : 0);
        result = 31 * result + (inequality ? 1 : 0);
        return result;
    }
}
