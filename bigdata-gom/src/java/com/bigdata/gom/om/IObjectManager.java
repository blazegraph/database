/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 19, 2012
 */

package com.bigdata.gom.om;

import org.openrdf.model.Resource;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.striterator.ICloseableIterator;

/**
 * The object manager is the abstraction for a connection the back end.
 */
public interface IObjectManager extends INativeTransaction {

    /**
     * Return a canonical {@link IGPO} for the {@link Resource} (canonical
     * within the scope of this object manager) and never <code>null</code>.
     * 
     * @param id
     *            The {@link Resource}.
     * 
     * @return The {@link IGPO}.
     * 
     * @see IGPO#getId()
     */
    /*
     * TODO We need to work through lazy materialization, materialization of
     * facets corresponding to class model information, and reconcilation of
     * property values for an object on commit with respect to the class model
     * information.
     * 
     * TODO How can we deal with blank nodes in this context?
     */
    // Return canonical IGPO for Resource (including Statement) w/in OM scope and never null.
    IGPO getGPO(Resource id);

    // Ensure Statements are materialized for gpo's Resource.
    void materialize(IGPO gpo);

    /**
     * Close the object manager, which terminates its connection with the
     * backing store. Any open concurrent or nested/native transactions are
     * flushed to the store.
     * 
     * @exception IllegalStateException
     *                if the store is already closed.
     */
    void close();

    /**
     * Return <code>true</code> iff the object manager is backed by some
     * persistence layer.
     */
    boolean isPersistent();

    /**
     * 
     */
    ICloseableIterator<BindingSet> evaluate(String queryStr);

    /**
     * 
     */
    void execute(String updateStr);

}
