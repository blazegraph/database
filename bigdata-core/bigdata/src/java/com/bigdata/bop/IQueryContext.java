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
 * Created on Aug 6, 2011
 */

package com.bigdata.bop;

import java.util.UUID;

import com.bigdata.htree.HTree;
import com.bigdata.rwstore.sector.IMemoryManager;

/**
 * Context for the evaluation of a query pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IQueryContext {

    /**
     * The unique identifier for the query.
     */
    UUID getQueryId();
    
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

    /**
     * Return an interface which allows attribute values to be associated with
     * an {@link IQueryContext}.
     */
    IQueryAttributes getAttributes();
    
}
