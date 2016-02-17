/*

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
 * Created on Mar 10, 2008
 */

package com.bigdata.journal;

import java.util.concurrent.Callable;

import com.bigdata.btree.IIndex;
import com.bigdata.journal.AbstractTask.InnerWriteServiceCallable;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * Interface available to tasks running under the {@link ConcurrencyManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ITask<T> extends Callable<T> {

    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    IResourceManager getResourceManager();

    /**
     * The journal against which the operation will be carried out.
     * <p>
     * If the task is running against an {@link ITx#UNISOLATED} index, then this
     * will be the {@link IResourceManager#getLiveJournal()}. If the operation
     * is a historical read, then it will be whatever journal is appropriate to
     * the historical commit point against which the task is being run.
     * <p>
     * Note: For {@link ITx#UNISOLATED} operations this exposes unconstrained
     * access to the journal that could be used to violate the concurrency
     * control mechanisms, therefore you SHOULD NOT use this unless you have a
     * clear idea what you are about. You should be able to write all
     * application level tasks in terms of {@link #getIndex(String)} and
     * operations on the returned index.
     * <p>
     * Note: For example, if you use the returned object to access a named index
     * and modify the state of that named index, your changes WILL NOT be
     * noticed by the checkpoint protocol in {@link InnerWriteServiceCallable}.
     * 
     * @return The corresponding journal for that timestamp -or-
     *         <code>null</code> if no journal has data for that timestamp,
     *         including when a historical journal with data for that timestamp
     *         has been deleted.
     * 
     * @see IResourceManager#getJournal(long)
     */
    IJournal getJournal();

    /**
     * Returns a copy of the array of resources declared to the constructor.
     */
    String[] getResource();

    /**
     * Return the only declared resource.
     * 
     * @return The declared resource.
     * 
     * @exception IllegalStateException
     *                if more than one resource was declared.
     */
    String getOnlyResource();

    /**
     * Returns <code>Task{taskName,timestamp,resource[]}</code>
     */
    @Override
    String toString();

    /**
     * Return an appropriate view of the named B+Tree that has the appropriate
     * isolation level for the operation (non-GIST).
     * <p>
     * When the task is isolated by a transaction, then the index will be
     * isolated by the transaction using the appropriate isolation level. If the
     * transaction is read-only, then the index will not be writable.
     * </p>
     * <p>
     * When the task is a read-only unisolated operation, the index will be
     * read-only and will read from the most recent committed state of the store
     * prior to the time at which the task began to execute. If multiple index
     * views are requested they will all use the same committed state of the
     * store.
     * </p>
     * <p>
     * When the task is an unisolated write operation the index will be the
     * unisolated writable (aka "live" or "current" index). Access to the
     * unisolated writable indices is single-threaded. This constraint is
     * enforced by a lock system using the named resources declared in the task
     * constructor.
     * </p>
     * 
     * @param name
     *            The index name.
     * 
     * @return An appropriate view of the named index.
     * 
     * @throws NullPointerException
     *             if <i>name</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if <i>name</i> is not a declared resource.
     * @throws StaleLocatorException
     *             if <i>name</i> identifies an index partition which has been
     *             split, joined, or moved.
     * @throws NoSuchIndexException
     *             if the named index is not registered as of the timestamp.
     * 
     *             TODO modify to return <code>null</code> if the index is not
     *             registered?
     */
    IIndex getIndex(String name); // non-GIST
    
    /**
     * Return an appropriate view of the named index for the operation (GIST).
     * <p>
     * This method MUST be used to access non-B+Tree data structures that do not
     * (yet) support {@link FusedView} style transaction isolation.
     * <p>
     * This method MAY NOT be used to access data structures if the operation is
     * isolated by a read-write transaction.
     * <p>
     * This method DOES NOT understand the ordered views used by scale-out. The
     * {@link ICheckpointProtocol} interface returned by this method is a
     * concrete durable GIST data structure with a specific commit record. It is
     * NOT a {@link FusedView} or similar data structure assembled from an
     * ordered array of indices. If this method is used for a GIST data
     * structure it will ONLY return the {@link ICheckpointProtocol} and will
     * not wrap it with a {@link FusedView}. (This is of practical importance
     * only for scale-out which uses {@link FusedView}s to support the dynamic
     * key range partitioning algorithm for the distributed B+Tree data
     * structure.)
     * 
     * @param name
     *            The index name.
     * 
     * @return An appropriate view of the named index.
     * 
     * @throws NullPointerException
     *             if <i>name</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if <i>name</i> is not a declared resource.
     * @throws StaleLocatorException
     *             if <i>name</i> identifies an index partition which has been
     *             split, joined, or moved.
     * @throws NoSuchIndexException
     *             if the named index is not registered as of the timestamp.
     * @throws UnsupportedOperationException
     *             if the {@link ITask} is associated with a read-write
     *             transaction.
     * 
     *             TODO modify to return <code>null</code> if the index is not
     *             registered?
     */
//    ICheckpointProtocol getIndexLocal(String name); // GIST

    /**
     * The object used to track events and times for the task.
     */
    TaskCounters getTaskCounters();
    
}
