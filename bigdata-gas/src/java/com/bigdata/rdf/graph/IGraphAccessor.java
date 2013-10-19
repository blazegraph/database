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

import java.util.Iterator;
import java.util.Random;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.impl.util.VertexDistribution;

/**
 * Interface abstracts access to a backend graph implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGraphAccessor {

    /**
     * Return the #of edges of the specified type for the given vertex.
     * <p>
     * Note: This is not always a flyweight operation due to the need to filter
     * for only the observable edge types. If this information is required, it
     * may be best to cache it on the vertex state object for a given
     * {@link IGASProgram}.
     * 
     * @param ctx
     *            The {@link IGASContext}.
     * @param u
     *            The vertex.
     * @param edges
     *            Typesafe enumeration indicating which edges should be visited.
     * 
     * @return An iterator that will visit the edges for that vertex.
     */
    long getEdgeCount(IGASContext<?, ?, ?> ctx, Value u, EdgesEnum edges);

    /**
     * Return the edges for the given vertex.
     * 
     * @param ctx
     *            The {@link IGASContext}.
     * @param u
     *            The vertex.
     * @param edges
     *            Typesafe enumeration indicating which edges should be visited.
     * 
     * @return An iterator that will visit the edges for that vertex.
     */
    Iterator<Statement> getEdges(IGASContext<?, ?, ?> ctx, Value u,
            EdgesEnum edges);

    /**
     * Hook to advance the view of the graph. This is invoked at the end of each
     * GAS computation round for a given {@link IGASProgram}.
     */
    void advanceView();

    /**
     * Obtain a weighted distribution of the vertices in the graph from which
     * samples may then be taken. The weight is the #of in-edges and out-edges
     * in which a given vertex appears. Statements that model property values
     * and statement that model link attributes SHOULD NOT be counted in this
     * sample.
     * 
     * @param r
     *            A random number generator that (a) MAY be used to select the
     *            sample distribution (the distribution does not need to be
     *            exhaustive so long as it obeys the sampling requirements); and
     *            (b) WILL be used to select vertices from that distribution.
     * 
     * @return The distribution.
     * 
     *         TODO SAMPLING: While the same distribution is used for all SAIL
     *         implementations, and while all implementations accept and use a
     *         random seed, there is NOT a guarantee the different
     *         implementations will sample the same vertices from the same
     *         graph. This is because the order in which the vertex sample is
     *         collected and the natural order of the vertices currently depends
     *         on the implementation objects.
     *         <p>
     *         This COULD be fixed if we required the implementation to put the
     *         sampled vertices into an order based on their external
     *         representation as RDF Values, but still reporting their internal
     *         since that is what is used to actually specify a starting vertex
     *         of the GAS program. However, even then, implementation specific
     *         characteristics might not provide us with the same vertices
     *         (e.g., bigdata imposes a canonical mapping on xsd:dateTime).
     */
    VertexDistribution getDistribution(Random r);

}
