package com.indeed.flamdex.query;

/**
 * @author jsgroth
 */
public class Term {
    private final String fieldName;
    private final boolean isIntField;
    private final long termIntVal;
    private final String termStringVal;

    public Term(String fieldName, boolean intField, long termIntVal, String termStringVal) {
        this.fieldName = fieldName;
        isIntField = intField;
        this.termIntVal = termIntVal;
        this.termStringVal = termStringVal;
    }

    public static Term intTerm(String field, long term) {
        return new Term(field, true, term, "");
    }

    public static Term stringTerm(String field, String term) {
        return new Term(field, false, 0, term);
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isIntField() {
        return isIntField;
    }

    public long getTermIntVal() {
        return termIntVal;
    }

    public String getTermStringVal() {
        return termStringVal;
    }

    @Override
    public String toString() {
        return (isIntField ? "int:" : "str:") + fieldName + ":" + (isIntField ? termIntVal : termStringVal);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Term)) return false;
        Term other = (Term) o;

        if (!fieldName.equals(other.fieldName)) return false;
        if (isIntField != other.isIntField) return false;

        if (isIntField) {
            if (termIntVal != other.termIntVal) return false;
        } else {
            if (termStringVal == null ? other.termStringVal != null : !termStringVal.equals(other.termStringVal)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = fieldName.hashCode();
        hashCode *= 31;
        hashCode += isIntField ? 1231 : 1237;

        if (isIntField) {
            hashCode *= 31;
            hashCode += termIntVal;
            hashCode *= 31;
            hashCode += termIntVal>>>32;
        } else {
            hashCode *= 31;
            if (termStringVal != null) hashCode += termStringVal.hashCode();
        }

        return hashCode;
    }
}
