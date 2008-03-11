/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

 Contact:
 SYSTAP, LLC
 4501 Tower Road
 Greensboro, NC 27410
 licenses@bigdata.com

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
import com.bigdata.resources.StaleLocatorException;

/**
 * Interface available to tasks running under the {@link ConcurrencyManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITask extends Callable<Object> {

    /**
     * The object used to manage access to the resources from which views of the
     * indices are created.
     */
    public IResourceManager getResourceManager();

    /**
     * The journal against which the operation will be carried out.
     * <p>
     * If the task is running against an unisolated index, then this will be the
     * {@link IResourceManager#getLiveJournal()}. Otherwise it will be whatever
     * journal is appropriate to the historical commit point against which the
     * task is being run.
     * <p>
     * Note: This exposes unconstrained access to the journal that could be used
     * to violate the concurrency control mechanisms, therefore you SHOULD NOT
     * use this unless you have a clear idea what you are about. You should be
     * able to write all application level tasks in terms of
     * {@link #getIndex(String)} and operations on the returned index. Using
     * {@link SequenceTask} you can combine application specific unisolated
     * write tasks with tasks that add or drop indices into atomic operations.
     * 
     * @return The corresponding journal for that timestamp -or-
     *         <code>null</code> if no journal has data for that timestamp,
     *         including when a historical journal with data for that timestamp
     *         has been deleted.
     *         
     * @see IResourceManager#getJournal(long)
     */
    public AbstractJournal getJournal();

    /**
     * Returns a copy of the array of resources declared to the constructor.
     */
    public String[] getResource();

    /**
     * Return the only declared resource.
     * 
     * @return The declared resource.
     * 
     * @exception IllegalStateException
     *                if more than one resource was declared.
     */
    public String getOnlyResource();

    /**
     * Returns Task{taskName,timestamp,resource[]}
     */
    public String toString();

    /**
     * Return an appropriate view of the named index for the operation.
     * <p>
     * When the task is isolated by a transaction, then the index will be
     * isolated by the transaction using the appropriate
     * {@link IsolationEnum isolation level}. If the transaction is read-only,
     * then the index will not be writable.
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
     * @exception NoSuchIndexException
     *                if the named index does not exist at the time that the
     *                operation is executed.
     * 
     * @exception StaleLocatorException
     *                if the named index does not exist at the time the
     *                operation is executed and the {@link IResourceManager} has
     *                information which indicates that the index partition has
     *                been split, joined or moved.
     * 
     * @exception IllegalStateException
     *                if the named index is not one of the resources declared to
     *                the constructor.
     */
    public IIndex getIndex(String name);

}