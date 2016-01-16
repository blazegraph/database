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

import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.isolation.IsolatedFusedView;
import com.bigdata.service.ITxState;

/**
 * <p>
 * Interface for transaction state on the client.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ITx extends ITxState {

    /**
     * The constant that SHOULD used as the timestamp for an <em>unisolated</em>
     * read-write operation. The value of this constant is ZERO (0L) -- this
     * value corresponds to <code>Wed Dec 31 19:00:00 EST 1969</code> when
     * interpreted as a {@link Date}.
     */
    public static final long UNISOLATED = 0L;
    
    /**
     * A constant that SHOULD used as the timestamp for <em>read-committed</em>
     * (non-transactional dirty reads) operations. The value of this constant is
     * MINUS ONE (-1L) -- this value corresponds to
     * <code>Wed Dec 31 18:59:59 EST 1969</code> when interpreted as a
     * {@link Date}.
     * <p>
     * If you want a scale-out index to be read consistent over multiple
     * operations, then use {@link IIndexStore#getLastCommitTime()} when you
     * specify the timestamp for the view. The index will be as of the specified
     * commit time and more recent commit points will not become visible.
     * <p>
     * {@link AbstractTask}s that run with read-committed isolation provide a
     * read-only view onto the most recently committed state of the indices on
     * which they read. However, when a process runs a series of
     * {@link AbstractTask}s with read-committed isolation the view of the
     * index in each distinct task will change if concurrenct processes commit
     * writes on the index (some constructs, such as the scale-out iterators,
     * provide read-consistent views for the last commit time). Further, an
     * index itself can appear or disappear if concurrent processes drop or
     * register that index.
     * <p>
     * A read-committed transaction imposes fewer constraints on when old
     * resources (historical journals and index segments) may be released. For
     * this reason, a read-committed transaction is a good choice when a
     * very-long running read must be performed on the database. Since a
     * read-committed transaction does not allow writes, the commit and abort
     * protocols are identical.
     * <p>
     * However, split/join/move operations can cause locators to become invalid
     * for read-committed (and unisolated) operations. For this reason, it is
     * often better to specify "read-consistent" semantics by giving the
     * lastCommitTime for the {@link IIndexStore}.
     */
    public static final long READ_COMMITTED = -1L;

    /**
     * When true, the transaction has an empty write set.
     */
    boolean isEmptyWriteSet();
    
    /**
     * Return an isolated view onto a named index. The index will be isolated at
     * the same level as this transaction. Changes on the index will be made
     * restart-safe iff the transaction successfully commits. Writes on the
     * returned index will be isolated in an {@link IsolatedFusedView}. Reads that
     * miss on the {@link IsolatedFusedView} will read through named index as of the
     * ground state of this transaction. If the transaction is read-only then
     * the index will not permit writes.
     * <p>
     * During {@link #prepare(long)}, the write set of each
     * {@link IsolatedFusedView} will be validated against the then current committed
     * state of the named index.
     * <p>
     * During {@link #mergeDown()}, the validated write sets will be merged down
     * onto the then current committed state of the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> if no index is registered
     *         under that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     *                
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    ILocalBTreeView getIndex(String name);
    
    /**
     * Return an array of the resource(s) (the named indices) on which the
     * transaction has written (the isolated index(s) that absorbed the writes
     * for the transaction).
     */
    String[] getDirtyResource();
    
}
