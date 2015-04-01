/*
 * Copyright (C) 2014 Indeed Inc.
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
package com.indeed.imhotep.local;

import com.indeed.flamdex.api.IntValueLookup;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * Intended exclusively for use by ImhotepLocalSession, hence package
 * private access for all operations. This is just a wrapper around an
 * array of IntValueLookups that fires a property change event
 * whenever a value it is changed. The event fired is always the same,
 * as more granular knowledge of changes is not useful in the context
 * of ImhotepLocalSession.
 *
 * @author johnf
 */
class StatLookup
{
    private final PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);
    private final PropertyChangeEvent   changeEvent   = new PropertyChangeEvent(this, "lookupChange", null, null);

    private final IntValueLookup[] lookups;

    StatLookup(final int numLookups) {
        this.lookups = new IntValueLookup[numLookups];
    }

    int length() { return lookups.length; }

    IntValueLookup get(final int index) { return lookups[index]; }

    void set(final int index, final IntValueLookup lookup) {
        lookups[index] = lookup;
        this.changeSupport.firePropertyChange(changeEvent);
    }

    void addPropertyChangeListener(PropertyChangeListener listener) {
        this.changeSupport.addPropertyChangeListener(listener);
    }

    void removePropertyChangeListener(PropertyChangeListener listener) {
        this.changeSupport.removePropertyChangeListener(listener);
    }
}
