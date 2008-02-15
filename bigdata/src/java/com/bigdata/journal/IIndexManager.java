/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 17, 2007
 */

package com.bigdata.journal;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;

/**
 * Interface for managing named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexManager extends IIndexStore {

    /**
     * Register a named index (unisolated). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @exception IllegalStateException
     *                if there is an index already registered under that name.
     */
    public IIndex registerIndex(String name);

    /**
     * Register a named index (unisolated). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered before it may be used inside of a
     * transaction.
     * <p>
     * Note: The return object MAY differ from the supplied {@link BTree}. For
     * example, when using partitioned indices the {@link BTree} is encapsulated
     * within an abstraction that knows how to managed index partitions.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @param btree
     *            The btree.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @exception IndexExistsException
     *                if there is an index already registered under that name.
     *                Use {@link IIndexStore#getIndex(String)} to test whether
     *                there is an index registered under a given name.
     * 
     * @todo The provided {@link BTree} must serve as a prototype so that it is
     *       possible to retain additional metadata.
     */
    public IIndex registerIndex(String name, BTree btree);

    /**
     * Drops the named index (unisolated). The index will no longer participate
     * in atomic commits.
     * <p>
     * Note: Whether or not and when index resources are reclaimed is dependent
     * on the store. For example, an immortal store will retain all historical
     * states for all indices. Likewise, a store that uses index partitions may
     * be able to delete index segments immediately.
     * 
     * @param name
     *            The name of the index to be dropped.
     * 
     * @exception NoSuchIndexException
     *                if <i>name</i> does not identify a registered index.
     * 
     * @todo add a rename index method, but note that names in the file system
     *       would not change.
     * 
     * @todo declare a method that returns or visits the names of the registered
     *       indices.
     */
    public void dropIndex(String name);
    
}
