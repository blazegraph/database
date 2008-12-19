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

import com.bigdata.btree.IndexMetadata;

/**
 * Interface for managing named indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexManager extends IIndexStore {

    /**
     * Register a named index.
     * <p>
     * Note: The <i>name</i> property MUST be set on the {@link IndexMetadata}
     * and the index will be registered under that name.
     * 
     * @param indexMetadata
     *            The metadata describing the index.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the name argument was not specified when the
     *             {@link IndexMetadata} was created.
     * 
     * @exception IndexExistsException
     *                if there is an index already registered under the name
     *                returned by {@link IndexMetadata#getName()}. Use
     *                {@link IIndexStore#getIndex(String)} to test whether there
     *                is an index registered under a given name.
     */
    public void registerIndex(IndexMetadata indexMetadata);

    /**
     * Drops the named index.
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
     */
    public void dropIndex(String name);
    
}
