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
package com.bigdata.ha;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.bigdata.journal.IIndexManager;

/**
 * Interface allows arbitrary tasks to be submitted to an {@link HAGlue} service
 * for evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 */
public interface IIndexManagerCallable<T> extends Serializable, Callable<T> {

    /**
     * Invoked before the task is executed to provide a reference to the
     * {@link IIndexManager} on which it is executing.
     * 
     * @param indexManager
     *            The index manager on the service.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws IllegalStateException
     *             if {@link #setIndexManager(IIndexManager)} has already set.
     */
    void setIndexManager(IIndexManager indexManager);

    /**
     * Return the {@link IIndexManager}.
     * 
     * @return The index manager and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if index manager reference is not set.
     */
    IIndexManager getIndexManager();

}
