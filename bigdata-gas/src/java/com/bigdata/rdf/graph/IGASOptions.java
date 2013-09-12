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

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import cutthecrap.utils.striterators.IStriterator;

/**
 * Interface for options that are understood by the {@link IGASEngine} and which
 * may be declared by the {@link IGASProgram}.
 * 
 * TODO Add option to order the vertices to provide a serializable execution
 * plan (like GraphChi). I believe that this reduces to computing a DAG over the
 * frontier before executing the GATHER and then executing the frontier such
 * that the parallel execution is constrained by arcs in the DAG that do not
 * have mutual dependencies. This is really an option that would be implemented
 * by the {@link IGASContext}, which would have to place a partial ordering over
 * the vertices in the frontier and then process the frontier with limited
 * parallelism based on that partial ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASOptions<VS, ES, ST> {

    /**
     * Return the set of edges to which the GATHER is applied -or-
     * {@link EdgesEnum#NoEdges} to skip the GATHER phase.
     */
    EdgesEnum getGatherEdges();

    /**
     * Return the set of edges to which the SCATTER is applied -or-
     * {@link EdgesEnum#NoEdges} to skip the SCATTER phase.
     */
    EdgesEnum getScatterEdges();

    /**
     * Return a factory for vertex state objects.
     * <p>
     * Note: A <code>null</code> value may not be allowed in the visited vertex
     * map, so if the algorithm does not use vertex state, then the factory
     * should return a singleton instance each time it is invoked.
     */
    Factory<Value, VS> getVertexStateFactory();

    /**
     * Return a factory for edge state objects -or- <code>null</code> if the
     * {@link IGASProgram} does not use edge state (in which case the edge state
     * will not be allocated or maintained).
     */
    Factory<Statement, ES> getEdgeStateFactory();

    /**
     * Return non-<code>null</code> iff there is a single link type to be
     * visited. This corresponds to a view of the graph as sparse connectivity
     * matrix. The {@link IGASEngine} can optimize traversal patterns using the
     * <code>POS</code> index.
     * <p>
     * Note: When this option is used, the scatter and gather will not visit the
     * property set for the vertex. The graph is treated as if it were an
     * unattributed graph and only mined for the connectivity data.
     * 
     * @return The {@link Value} for the predicate that identifies the desired
     *         link type (there can be many types of links - the return value
     *         specifies which attribute is of interest).
     * 
     * @see #getLinkAttribType()
     */
    URI getLinkType();

    /**
     * Hook to impose a constraint on the visited edges and/or property values.
     * 
     * @param itr
     *            The iterator visiting those edges and/or property values.
     * 
     * @return Either the same iterator or a constrained iterator.
     * 
     *         TODO Rename as constrainEdgeFilter or even split into a
     *         constrainGatherFilter and a constraintScatterFilter.
     * 
     *         FIXME APPLY : If we need access to the vertex property values in
     *         APPLY (which we probably do, at least optionally), then there
     *         should be a similar method to decide whether the property values
     *         for the vertex are made available during the APPLY.
     */
    IStriterator constrainFilter(IGASContext<VS, ES, ST> ctx, IStriterator eitr);
    
}