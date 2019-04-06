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

import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import lombok.EqualsAndHashCode;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Combine several iterators to one.
 * Result iterator values are sum of corresponding input iterator values.
 *
 * @author aibragimov
 */

@EqualsAndHashCode
public class GroupStatsIteratorCombiner extends AbstractLongIterator implements GroupStatsIterator {

    private static final Logger log = Logger.getLogger( GroupStatsIteratorCombiner.class);

    private final List<GroupStatsIterator> stats;
    private final int size;

    public GroupStatsIteratorCombiner(final GroupStatsIterator[] stats) {
        int size = 0;
        this.stats = new LinkedList<>();
        for( final GroupStatsIterator stat : stats ) {
            if( stat.hasNext() ) {
                this.stats.add( stat );
                size = Math.max(size, stat.getNumGroups());
            } else {
                Closeables2.closeQuietly( stat, log );
            }
        }

        this.size = size;
    }

    @Override
    public int getNumGroups() {
        return size;
    }

    @Override
    public boolean hasNext() {
        return !stats.isEmpty();
    }

    @Override
    public long nextLong() throws NoSuchElementException {

        if( stats.isEmpty() ) {
            throw new NoSuchElementException();
        }

        long result = 0;
        for(final Iterator<GroupStatsIterator> iter = stats.iterator(); iter.hasNext(); ) {
            final GroupStatsIterator stat = iter.next();
            result += stat.nextLong();
            if( !stat.hasNext() ) {
                Closeables2.closeQuietly(stat, log);
                iter.remove();
            }
        }

        return result;
    }

    @Override
    public void close(){
        Closeables2.closeAll( stats, log );
    }
}
