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
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.analytics.CC;
import com.bigdata.rdf.graph.impl.util.GASRunnerBase;

/**
 * Interface for options that are understood by the {@link IGASEngine} and which
 * may be declared by the {@link IGASProgram}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASOptions<VS, ES, ST> {

    /**
     * Return the nature of the initial frontier for this algorithm.
     */
    FrontierEnum getInitialFrontierEnum();

    /**
     * Return the type of edges that must exist when sampling the vertices of
     * the graph. If {@link EdgesEnum#InEdges} is specified, then each sampled
     * vertex will have at least one in-edge. If {@link EdgesEnum#OutEdges} is
     * specified, then each sampled vertex will have at least one out-edge. To
     * sample all vertices regardless of their edges, specify
     * {@value EdgesEnum#NoEdges}. To require that each vertex has at least one
     * in-edge and one out-edge, specify {@link EdgesEnum#AllEdges}.
     * 
     * FIXME This should be moved into {@link GASRunnerBase}. The only class
     * that customizes this is {@link CC}. (For {@link CC} we need to put all
     * vertices into the frontier, even those without edges.)
     */
    EdgesEnum getSampleEdgesFilter();
    
    /**
     * Return the set of edges to which the GATHER is applied for a
     * <em>directed</em> graph -or- {@link EdgesEnum#NoEdges} to skip the GATHER
     * phase. This will be interpreted based on the value reported by 
     * {@link IGASContext#isDirectedTraversal()}.
     * 
     * TODO We may need to set dynamically when visting the vertex in the
     * frontier rather than having it be a one-time property of the vertex
     * program.
     */
    EdgesEnum getGatherEdges();

    /**
     * Return the set of edges to which the SCATTER is applied for a
     * <em>directed</em> graph -or- {@link EdgesEnum#NoEdges} to skip the
     * SCATTER phase. This will be interpreted based on the value reported by
     * {@link IGASContext#isDirectedTraversal()}.
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

}