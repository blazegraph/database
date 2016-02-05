/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jan 10, 2008
 */
package com.bigdata.journal;

import com.bigdata.btree.proc.IIndexProcedure;

/**
 * Class provides an adaptor allowing a {@link IIndexProcedure} to be executed
 * on an {@link IConcurrencyManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class IndexProcedureTask<T> extends AbstractTask<T> {

    private final IIndexProcedure<T> proc;

    public IndexProcedureTask(final ConcurrencyManager concurrencyManager,
            final long startTime, final String name, final IIndexProcedure<T> proc) {

        super(concurrencyManager, startTime, name);

        if (proc == null)
            throw new IllegalArgumentException();

        this.proc = proc;

    }

    @Override
    final public T doTask() throws Exception {

        return proc.apply(getIndex(getOnlyResource()));

    }

    /**
     * Returns the name of the {@link IIndexProcedure} that is being executed.
     */
    @Override
    final protected String getTaskName() {
        
        return proc.getClass().getName();
        
    }
    
}
