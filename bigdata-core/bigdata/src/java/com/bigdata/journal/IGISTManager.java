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
 * Created on July 17, 2014
 */
package com.bigdata.journal;

import java.util.Iterator;

import com.bigdata.btree.IndexMetadata;

/**
 * Interface for managing local or distributed index structures in a manner that
 * is not B+Tree specific. These methods are thus GIST compatible, but that does
 * not guarantee distributed support for all GIST data structures.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585" > GIST
 *      </a>
 */
public interface IGISTManager {

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
     *                returned by {@link IndexMetadata#getName()}.
     * 
     * @see IGISTLocalManager#getIndexLocal(String, long)
     */
    void registerIndex(IndexMetadata indexMetadata);// GIST

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
    void dropIndex(String name); // GIST

    /**
     * Iterator visits the names of all indices spanned by the given prefix.
     * 
     * @param prefix
     *            The prefix (optional). When given, this MUST include a
     *            <code>.</code> if you want to restrict the scan to only those
     *            indices in a given namespace. Otherwise you can find indices
     *            in <code>kb2</code> if you provide the prefix <code>kb</code>
     *            where both kb and kb2 are namespaces since the indices spanned
     *            by <code>kb</code> would include both <code>kb.xyz</code> and
     *            <code>kb2.xyx</code>.
     * @param timestamp
     *            A timestamp which represents either a possible commit time on
     *            the store or a read-only transaction identifier.
     * 
     * @return An iterator visiting those index names.
     */
    Iterator<String> indexNameScan(String prefix, long timestamp); // GIST

}
