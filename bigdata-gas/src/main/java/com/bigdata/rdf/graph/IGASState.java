/**
   Copyright (C) SYSTAP, LLC 2006-2016.  All rights reserved.

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

import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Interface exposes access to the VS and ES that is visible during a GATHER or
 * SCATTER operation.
 * <p>
 * This interface is intended to be restrictive in both its API and the state
 * that the API will expose in order to facilitate scaling in multi-machine
 * environments.
 * <p>
 * A concrete implementation of this interface for a cluster WILL ONLY provide
 * O(1) access to vertices whose state has not been materialized on a given node
 * by a GATHER or SCATTER. State for vertices that were not materialized will
 * not be accessible.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @param <VS>
 *            The generic type for the per-vertex state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ES>
 *            The generic type for the per-edge state. This is scoped to the
 *            computation of the {@link IGASProgram}.
 * @param <ST>
 *            The generic type for the SUM. This is often directly related to
 *            the generic type for the per-edge state, but that is not always
 *            true. The SUM type is scoped to the GATHER + SUM operation (NOT
 *            the computation).
 */
public interface IGASState<VS,ES, ST> {

    /**
     * {@link #reset()} the computation state and populate the initial frontier.
     * 
     * @param ctx
     *            The execution context.
     * @param v
     *            One or more vertices that will be included in the initial
     *            frontier.
     * 
     * @throws IllegalArgumentException
     *             if no vertices are specified.
     */
    void setFrontier(IGASContext<VS, ES, ST> ctx, Value... v);

    /**
     * Discard computation state (the frontier, vertex state, and edge state)
     * and reset the round counter.
     * <p>
     * Note: The graph is NOT part of the computation and is not discared by
     * this method.
     */
    void reset();
    
    /**
     * Return the current evaluation round (origin ZERO).
     */
    int round();
 
    /**
     * Get the state for the vertex using the appropriate factory. If this is
     * the first visit for that vertex, then the state is initialized using the
     * factory. Otherwise the existing state is returned.
     * 
     * @param v
     *            The vertex.
     * 
     * @return The state for that vertex.
     * 
     * @see IGASProgram#getVertexStateFactory()
     */
    VS getState(Value v);

    /**
     * Get the state for the edge using the appropriate factory. If this is the
     * first visit for that vertex, then the state is initialized using the
     * factory. Otherwise the existing state is returned.
     * 
     * @param v
     *            The vertex.
     * 
     * @return The state for that vertex.
     * 
     * @see IGASProgram#getEdgeStateFactory()
     */
    ES getState(Statement e);

    /**
     * Return <code>true</code> iff the specified vertex has an associated
     * vertex state object - this is interpreted as meaning that the vertex has
     * been "visited".
     * 
     * @param v
     *            The vertex.
     * @return <code>true</code> iff there is vertex state associated with that
     *         vertex.
     */
    boolean isVisited(Value v);
    
    /**
     * Return <code>true</code> iff the specified vertices all have an associated
     * vertex state object - this is interpreted as meaning that the vertex has
     * been "visited".
     * 
     * @param v
     *            The vertices.
     * @return <code>true</code> iff there is vertex state associated with all
     *         specified vertices.
     */
    boolean isVisited(Set<Value> v);
    
    /**
     * The current frontier.
     */
    IStaticFrontier frontier();

    /**
     * Return the {@link IGASSchedulerImpl}.
     */
    IGASSchedulerImpl getScheduler();

    /**
     * Compute a reduction over the vertex state table (all vertices that have
     * had their vertex state materialized).
     * 
     * @param op
     *            The reduction operation.
     * 
     * @return The reduction.
     */
    <T> T reduce(IReducer<VS, ES, ST, T> op);

    /**
     * End the current round, advance the round counter, and compact the new
     * frontier.
     */
    void endRound();

    /**
     * Conditionally log various interesting information about the state of the
     * computation.
     */
    void traceState();

    /**
     * Return the other end of a link.
     * 
     * @param u
     *            One end of the link.
     * @param e
     *            The link.
     * 
     * @return The other end of the link.
     */
    Value getOtherVertex(Value u, Statement e);
    
    /**
     * Return the link attribute, if there is one.
     * 
     * @param u
     *            One end of the link.
     * @param e
     *            The link.
     * 
     * @return The other end of the link.
     */
    Literal getLinkAttr(Value u, Statement e);
    
    /**
     * Return a useful representation of an edge (non-batch API, debug only).
     * This method is only required when the edge objects are internal database
     * objects lacking fully materialized RDF {@link Value}s. In this case, it
     * will materialize the RDF Values and present a pleasant view of the edge.
     * The materialization step is a random access, which is why this method is
     * for debug only. Efficient, vectored mechanisms exist to materialize RDF
     * {@link Value}s for other purposes, e.g., when exporting a set of edges as
     * as graph in a standard interchange syntax.
     * 
     * @param e
     *            The edge.
     * @return The representation of that edge.
     */
    String toString(Statement e);

    /**
     * Return <code>true</code> iff the given {@link Statement} models an edge
     * that connects two vertices ({@link Statement}s also model property
     * values).
     * 
     * @param e
     *            The statement.
     *            
     * @return <code>true</code> iff that {@link Statement} is an edge of the
     *         graph.
     */
    boolean isEdge(final Statement e);
    
    /**
     * Return <code>true</code> iff the given {@link Statement} models an
     * property value for a vertex of the graph ({@link Statement}s also model
     * edges).
     * 
     * @param e
     *            The statement.
     * @return <code>true</code> iff that {@link Statement} is an edge of the
     *         graph.
     */
    boolean isAttrib(Statement e);
    
    /**
     * Return <code>true</code> iff the statement models a link attribute having
     * the specified link type. When this method returns <code>true</code>, the
     * {@link Statement#getSubject()} may be decoded to obtain the link
     * described by that link attribute using {@link #decodeStatement(Value)}.
     * 
     * @param e
     *            The statement.
     * @param linkAttribType
     *            The type for the link attribute.
     * 
     * @return <code>true</code> iff the statement is an instance of a link
     *         attribute for the specified link type.
     */
    boolean isLinkAttrib(Statement e, URI linkAttribType);
    
    /**
     * If the vertex is actually an edge, then return the decoded edge.
     * <p>
     * Note: A vertex may be an edge. A link attribute is modeled by treating
     * the link as a vertex and then asserting a property value about that
     * "link vertex". For bigdata, this is handled efficiently as inline
     * statements about statements. This approach subsumes the property graph
     * model (property graphs do not permit recursive nesting of these
     * relationships) and is 100% consistent with RDF reification, except that
     * the link attributes are modeled efficiently inline with the links. This
     * is what we call <a
     * href="http://www.bigdata.com/whitepapers/reifSPARQL.pdf" > Reification
     * Done Right </a>.
     * 
     * @param v
     *            The vertex.
     * 
     * @return The edge decoded from that vertex and <code>null</code> iff the
     *         vertex is not an edge.
     * 
     * @see <a href="http://www.bigdata.com/whitepapers/reifSPARQL.pdf" >
     *      Reification Done Right </a>
     *      
     * @see <a href="http://wiki.blazegraph.com/wiki/index.php/RDF_GAS_API" > RDF
     *      GAS API</a>
     */
    Statement decodeStatement(Value v);
 
    /**
     * Return -1, o, or 1 if <code>u</code> is LT, EQ, or GT <code>v</code>. A
     * number of GAS programs depend on the ability to place an order over the
     * vertex identifiers, as does 2D partitioning. The ordering provided by
     * this method MAY be arbitrary, but it MUST be total and stable across the
     * life-cycle of the GAS program evaluation.
     * 
     * @param u
     *            A vertex.
     * @param v
     *            Another vertex.
     */
    int compareTo(Value u, Value v);

    /**
     * Retain only those vertices in the visited set that are found in the
     * specified collection.
     * 
     * @param retainSet The set of vertices to be retained.
     */
    void retainAll(Set<Value> retainSet);

    /**
	 * Convert a value into an appropriate internal form.
	 * 
	 * @param value
	 *            The value.
	 *            
	 * @return The internal form and <code>null</code> if the argument is
	 *         <code>null</code>.
	 */
	Value asValue(Value value);

}
