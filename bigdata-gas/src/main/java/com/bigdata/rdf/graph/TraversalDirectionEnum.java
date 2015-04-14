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
 * Typesafe enumeration of manner in which an RDF graph will be traversed by an
 * {@link IGASProgram} based on its {@link EdgesEnum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public enum TraversalDirectionEnum {

    /**
     * Directed traversal along the natural direction of the RDF statements
     * (from Subject to Object).
     */
    Forward,
    /**
     * Directed traversal along the reverse direction of the RDF statements
     * (from Object to Subject).
     */
    Reverse,
    /**
     * Undirected traversal - edges are explored in both the {@link #Forward}
     * and the {@link #Reverse} direction.
     */
    Undirected;

    /**
     * Interpret the given {@link EdgesEnum}, returning the effective value
     * required to impose the semantics of this {@link TraversalDirectionEnum}.
     * 
     * @param edges
     *            The {@link EdgesEnum}.
     *            
     * @return The effective {@link EdgesEnum} value that will impose the
     *         traversal semantics of this {@link TraversalDirectionEnum}.
     *         
     * @see EdgesEnum#asUndirectedTraversal()
     */
    public EdgesEnum asTraversed(final EdgesEnum edges) {

        switch (this) {
        case Forward:
            return edges;
        case Reverse:
            switch (edges) {
            case InEdges:
                return EdgesEnum.OutEdges;
            case OutEdges:
                return EdgesEnum.InEdges;
            default:
                return edges;
            }
        case Undirected:
            return edges.asUndirectedTraversal();
        default:
            throw new AssertionError();
        }

    }

}
