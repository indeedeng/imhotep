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

public class RegroupCondition {
    public final String field;
    public final boolean intType;
    public final long intTerm;
    public final String stringTerm;
    public final boolean inequality;

    public RegroupCondition(final String field, final boolean intType, final long intTerm, final String stringTerm, final boolean inequality) {
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RegroupCondition that = (RegroupCondition) o;

        if (inequality != that.inequality) {
            return false;
        }
        if (intTerm != that.intTerm) {
            return false;
        }
        if (intType != that.intType) {
            return false;
        }
        if (field != null ? !field.equals(that.field) : that.field != null) {
            return false;
        }
        if (stringTerm != null ? !stringTerm.equals(that.stringTerm) : that.stringTerm != null) {
            return false;
        }

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
