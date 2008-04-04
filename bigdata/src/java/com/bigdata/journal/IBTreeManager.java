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
 * Created on Apr 4, 2008
 */

package com.bigdata.journal;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.service.IDataService;

/**
 * Extended to allow direct registration of a named {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBTreeManager extends IIndexManager {

    /**
     * Register a named index.
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
     */
    public IIndex registerIndex(String name, BTree btree);

    /**
     * Register a named index.
     * <p>
     * This variant allows you to register an index under a name other than the
     * value returned by {@link IndexMetadata#getName()}.
     * <p>
     * Note: This variant is generally by the {@link IMetadataService} when it
     * needs to register a index partition of some scale-out index on a
     * {@link IDataService}. In this case the <i>name</i> is the name of the
     * index partition while the value reported by
     * {@link IndexMetadata#getName()} is the name of the scale-out index. In
     * nearly all other cases you can use {@link #registerIndex(IndexMetadata)}
     * instead. The same method signature is also declared by
     * {@link IDataService#registerIndex(String, IndexMetadata)} in order to
     * support registration of index partitions.
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @param indexMetadata
     *            The metadata describing the index.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @exception IndexExistsException
     *                if there is an index already registered under that name.
     *                Use {@link IIndexStore#getIndex(String)} to test whether
     *                there is an index registered under a given name.
     */
    public IIndex registerIndex(String name, IndexMetadata indexMetadata);

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} unless discarded by
     * {@link #abort()}.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The named index or <code>null</code> iff there is no index
     *         registered with that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     */
    public IIndex getIndex(String name);

}
