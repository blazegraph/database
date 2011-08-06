/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 6, 2011
 */

package com.bigdata.bop;

import java.util.UUID;

import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.rwstore.sector.IMemoryManager;

/**
 * Context for the evaluation of a query pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO An operator which will buffer data using the
 *          {@link DirectBufferPoolAllocator} will also need access to the
 *          appropriate {@link DirectBufferPoolAllocator.IAllocationContext}.
 *          Access to the allocation context is by an appropriate allocation
 *          context key. [This is relevant for temporary solution sets if they
 *          are buffered on NIO chunks rather than on an HTree. Note that using
 *          the lower level NIO chunk mechanism will allow us to preserve the
 *          order of the solutions in a subquery while an HTree does not. Also,
 *          note that only the lower level NIO chunk mechanism allows the data
 *          to be transferred among nodes (we only move IBindingSet[] chunks,
 *          not higher level data structures).]
 */
public interface IQueryContext {

    /**
     * The unique identifier for the query.
     */
    UUID getQueryId();
    
//    /**
//     * Return the node-level attribute for this query. This may be used on a
//     * query controller to preserve metadata across subqueries. The attribute
//     * value is only visible on the query engine where it was defined.
//     */
//    Object getAttribute(String key);
//
//    /**
//     * Set a node-level attribute for this query. The attribute value is only
//     * visible on the query engine where it was defined.
//     * 
//     * @param key
//     * @param val
//     * @return
//     */
//    Object setAttribute(String key, Object val);

    /**
     * Return the {@link IMemoryManager} which may be used to buffer data on
     * high level data structures, such as the {@link HTree}, for this query.
     * Each operator in the query should in general create its own child
     * {@link IMemoryManager}. While the overall {@link IMemoryManager} context
     * associated with a query will be released when the query terminates,
     * operators which create child {@link IMemoryManager} contexts are
     * responsible for releasing their {@link IMemoryManager} in a timely
     * fashion when the operator has finished its evaluation.
     */
    IMemoryManager getMemoryManager();

}
