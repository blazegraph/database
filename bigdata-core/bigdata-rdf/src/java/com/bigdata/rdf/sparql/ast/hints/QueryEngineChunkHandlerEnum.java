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
 * Created on Aug 15, 2016
 */
package com.bigdata.rdf.sparql.ast.hints;

import com.bigdata.bop.engine.IChunkHandler;
import com.bigdata.bop.engine.ManagedHeapStandloneChunkHandler;
import com.bigdata.bop.engine.NativeHeapStandloneChunkHandler;

/**
 * Type safe enumeration of known {@link IChunkHandler} implementations in 
 * support of the {@link QueryEngineChunkHandlerQueryHint}.
 * 
 * @author bryan
 *
 * @see BLZG-533 (Vector query engine on the native heap)
 */
public enum QueryEngineChunkHandlerEnum {

    /**
     * Use the managed object heap (this is the historical behavior).
     */
    Managed,
    /**
     * Use the native heap.
     */
    Native;

    /**
     * Return the type safe enumeration corresponding to a specific
     * implementation class.
     * 
     * @param cls
     *            The {@link IChunkHandler} implementation class.
     *            
     * @return The corresponding type safe enum value.
     */
    public static QueryEngineChunkHandlerEnum valueOf(Class<? extends IChunkHandler> cls) {
        
        if (NativeHeapStandloneChunkHandler.class.getName().equals(cls.getName())) {

            return QueryEngineChunkHandlerEnum.Native;
            
        } else if (ManagedHeapStandloneChunkHandler.class.getName().equals(cls.getName())) {
            
            return QueryEngineChunkHandlerEnum.Managed;
            
        } else {
            
            throw new IllegalArgumentException(cls.getName());
            
        }
        
    }
}
