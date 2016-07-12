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
package com.bigdata.journal;

/**
 * An interface implemented by a persistence capable data structure such as a
 * btree so that it can participate in the commit protocol for the store.
 * <p>
 * This interface is invoked by {@link Journal#commit()} for each registered
 * {@link ICommitter}. The address returned by {@link #handleCommit()} will be
 * saved in the {@link ICommitRecord} under the index identified by the
 * {@link ICommitter} when it was registered.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see IAtomicStore#setCommitter(int, ICommitter)
 */
public interface ICommitter {

    /**
     * Flush dirty state to the store in preparation for an atomic commit and
     * return the address from which the persistence capable data structure may
     * be reloaded.
     * 
     * @param commitTime
     *            The timestamp assigned to the commit.
     * 
     * @return The address of the record from which the persistence capable data
     *         structure may be reloaded. If no changes have been made then the
     *         previous address should be returned as it is still valid.
     */
    public long handleCommit(long commitTime);
    
    /**
     * Mark an {@link ICommitter} as invalid. This will prevent it from allowing
     * any writes through to the backing store.
     * 
     * @param t
     *            A cause (required).
     * 
     * @see https://jira.blazegraph.com/browse/BLZG-1953
     */
    public void invalidate(Throwable t);

}
