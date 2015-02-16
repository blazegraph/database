/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph;

/**
 * Typesafe enumeration used to specify whether a GATHER or SCATTER phase is
 * applied to the in-edges, out-edges, both, or not run.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public enum EdgesEnum {
    
    /** The phase is not run. */
    NoEdges(false/* inEdges */, false/* outEdges */),
    
    /** The phase is applied only to the in-edges. */
    InEdges(true/* inEdges */, false/* outEdges */),
    
    /** The phase is applied only to the out-edges. */
    OutEdges(false/* inEdges */, true/* outEdges */),
    
    /** The phase is applied to all edges (both in-edges and out-edges). */
    AllEdges(true/* inEdges */, true/* outEdges */);

    private EdgesEnum(final boolean inEdges, final boolean outEdges) {
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }
    
    private final boolean inEdges;
    private final boolean outEdges;
    
    /**
     * Return <code>true</code>iff the in-edges will be visited.
     */
    public boolean doInEdges() {
        return inEdges;
    }

    /**
     * Return <code>true</code>iff the out-edges will be visited.
     */
    public boolean doOutEdges() {
        return outEdges;
    }

    /**
     * Return <code>true</code> iff the value is either {@link #InEdges} or
     * {@link #OutEdges}.
     */
    public boolean isDirected() {
        switch (this) {
        case NoEdges:
            /*
             * Note: None is neither directed nor non-directed regardless of the
             * value returned here. The caller should not be inquiring about
             * directedness of the GATHER or SCATTER unless they will be
             * executing that stage.
             */
            return false;
        case AllEdges:
            return false;
        case InEdges:
            return true;
        case OutEdges:
            return true;
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Promote an {@link EdgesEnum} value that was specified with the assumption
     * that the graph is directed into an {@link EdgesEnum} value that should be
     * used when the graph is undirected. There is no change for
     * {@link #NoEdges} and {@link #AllEdges}. If the value is either
     * {@link #InEdges} or {@link #OutEdges} then it is promoted to
     * {@link #AllEdges}.
     */
    public EdgesEnum asUndirectedTraversal() {
        switch (this) {
        case NoEdges:
        case AllEdges:
            // No change.
            return this;
        case InEdges: 
        case OutEdges:
            // promote to AllEdges.
            return AllEdges;
        default:
            throw new UnsupportedOperationException();
        }
    }
    
}