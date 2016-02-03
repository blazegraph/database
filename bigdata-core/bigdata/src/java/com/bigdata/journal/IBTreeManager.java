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
 * Created on Apr 4, 2008
 */

package com.bigdata.journal;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.view.FusedView;
import com.bigdata.htree.HTree;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.ndx.IClientIndex;

/**
 * Interface for management of local index resources such as {@link BTree},
 * {@link HTree}, etc.
 * 
 * TODO GIST This interface was historically only for the local {@link BTree}.
 * The GIST support has been pushed down into the {@link IGISTLocalManager}. The
 * {@link BTree} and {@link IIndex} specific methods on this interface are a
 * mixture of convenience and support for scale-out and should eventually be
 * phased out of this interface. The interface itself could be renamed to
 * something along the lines of "IIndexLocalManager".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585" > GIST
 *      </a>
 */
public interface IBTreeManager extends IIndexManager, IGISTLocalManager {

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
     * 
     * @see IIndexManager#registerIndex(IndexMetadata)
     * @see IIndexManager#getIndex(String, long)
     * @see IGISTLocalManager#getIndexLocal(String, long)
     */
    IIndex registerIndex(String name, BTree btree); // non-GIST

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
     * <p>
     * Note: Due to the method signature, this method CAN NOT be used to create
     * and register persistence capable data structures other than an
     * {@link IIndex} (aka B+Tree).
     * 
     * @param name
     *            The name that can be used to recover the index.
     * 
     * @param indexMetadata
     *            The metadata describing the index.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @throws IndexExistsException
     *             if there is an index already registered under that name.
     * 
     * @see IIndexManager#getIndex(String, long)
     * @see IGISTLocalManager#getIndexLocal(String, long)
     * 
     *      FIXME GIST Due to the method signature, this method CAN NOT be used
     *      to create and register persistence capable data structures other
     *      than an {@link IIndex} (aka B+Tree). It is difficult to reconcile
     *      this method with other method signatures since this method is
     *      designed for scale-out and relies on {@link IIndex}. However, only
     *      the B+Tree is an {@link IIndex}. Therefore, this method signature
     *      can not be readily reconciled with the {@link HTree}. The only
     *      interface which the {@link BTree} and {@link HTree} share is the
     *      {@link ICheckpointProtocol} interface, but that is a purely local
     *      (not remote) interface and is therefore not suitable to scale-out.
     *      Also, it is only in scale-out where the returned object can be a
     *      different type than the simple {@link BTree} class, e.g., a
     *      {@link FusedView} or even an {@link IClientIndex}.
     */
    IIndex registerIndex(String name, IndexMetadata indexMetadata); // non-GIST

    /**
     * Return the unisolated view of the named index (the mutable view of the
     * live index object).
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The unisolated view named index or <code>null</code> iff there
     *         is no index registered with that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     */
    IIndex getIndex(String name); // non-GIST

}
