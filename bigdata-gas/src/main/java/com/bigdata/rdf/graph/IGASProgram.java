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

/**
 * Abstract interface for GAS programs.
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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO DESIGN: The broad problem with this approach is that it is
 *         overly coupled with the Java object model. Instead it needs to expose
 *         an API that is aimed at vectored (for GPU) execution with 2D
 *         partitioning (for out-of-core, multi-node).
 */
public interface IGASProgram<VS, ES, ST> extends IGASOptions<VS, ES, ST>,
        IBindingExtractor<VS, ES, ST> {

    /**
     * One time initialization before the {@link IGASProgram} is executed.
     * 
     * @param ctx
     *            The evaluation context.
     */
    void before(IGASContext<VS, ES, ST> ctx);

//    /**
//     * Return a default reduction that will be applied after the
//     * {@link IGASProgram} is executed.
//     * 
//     * @return The default reduction -or- <code>null</code> if no such reduction
//     *         is defined.
//     */
//    <T> IReducer<VS, ES, ST, T> getDefaultAfterOp();

    /**
     * Callback to initialize the state for each vertex in the initial frontier
     * before the first iteration. A typical use case is to set the distance of
     * the starting vertex to ZERO (0).
     * 
     * @param u
     *            The vertex.
     * 
     *            TODO We do not need both the {@link IGASContext} and the
     *            {@link IGASState}. The latter is available from the former.
     */
    void initVertex(IGASContext<VS, ES, ST> ctx, IGASState<VS, ES, ST> state,
            Value u);

    /**
     * GATHER is a map/reduce over the edges of the vertex. The SUM provides
     * pair-wise reduction over the edges visited by the GATHER.
     * 
     * @param u
     *            The vertex for which the gather is being performed. The gather
     *            will be invoked for each edge indident on <code>u</code> (as
     *            specified by {@link #getGatherEdges()}).
     * @param e
     *            An edge (s,p,o).
     * 
     * @return The new edge state accumulant.
     * 
     *         FIXME DESIGN: The problem with pushing the ISPO onto the ES is
     *         that we are then forced to maintain edge state (for the purposes
     *         of accessing those ISPO references) even if the algorithm does
     *         not require any memory for the edge state!
     *         <p>
     *         Note: by lazily resolving the vertex and/or edge state in the GAS
     *         callback methods we avoid eagerly materializing data that we do
     *         not need. [Lazy resolution does not work on a cluster. The only
     *         available semantics there are lazy resolution of state that was
     *         materialized in order to support a gather() or scatter() for a
     *         vertex.]
     *         <p>
     *         Note: The state associated with the source/target vertex and the
     *         edge should all be immutable for the GATHER. The vertex state
     *         should only be mutable for the APPLY(). The target vertex state
     *         and/or edge state MAY be mutable for the SCATTER, but that
     *         depends on the algorithm. How can we get these constraints into
     *         the API?
     */
    ST gather(IGASState<VS, ES, ST> state, Value u, Statement e);
    
    /**
     * SUM is a pair-wise reduction that is applied during the GATHER.
     * 
     * @param left
     *            An edge state accumulant.
     * @param right
     *            Another edge state accumulant.
     * 
     * @return Their "sum".
     * 
     *         TODO DESIGN: Rather than pair-wise reduction, why not use
     *         vectored reduction? That way we could use an array of primitives
     *         as well as objects.
     * 
     *         TODO DESIGN: This should be a reduced interface since we only
     *         need access to the comparator semantics while the [state]
     *         provides random access to vertex and edge state. The comparator
     *         is necessary for MIN semantics for the {@link Value}
     *         implementation of the backend. E.g., Value versus IV.
     */
    ST sum(final IGASState<VS, ES, ST> state, ST left, ST right);

    /**
     * Apply the reduced aggregation computed by GATHER + SUM to the vertex.
     * 
     * @param u
     *            The vertex.
     * @param sum
     *            The aggregated accumulate across the edges as computed by
     *            GATHER and SUM -or- <code>null</code> if there is no
     *            accumulant (this will happen if the GATHER did not find any
     *            edges to visit).
     * 
     * @return The new state for the vertex.
     * 
     *         TODO How to indicate if there is no state change? return the same
     *         object? This only matters with secondary storage for the vertex
     *         state. Alternative is to side-effect the vertex state, but then
     *         we can not manage the barriers (BFS versus asynchronous). Except
     *         for indicating that the state is dirty, we do not need a return
     *         value. [The pattern appears to be that a vertex leaves a marker
     *         on its vertex state object indicating whether or not it was
     *         changed and then tests that state when deciding whether or not to
     *         scatter].
     * 
     *         TODO There could be a big win here if we are able to detect when
     *         a newly initialized vertex state does not "escape" and simply not
     *         store it. For some graphs, the vertexState map grows very rapidly
     *         when compared to either the frontier or the set of states that
     *         have been in the frontier during the computation.
     */
    VS apply(IGASState<VS, ES, ST> state, Value u, ST sum);

    /**
     * Return <code>true</code> iff the vertex should run its SCATTER phase.
     * This may be used to avoid visiting the edges if it is known (e.g., based
     * on the APPLY) that the vertex has not changed. This can save a
     * substantial amount of effort.
     * 
     * @param state
     * @param u
     *            The vertex.
     * @return
     */
    boolean isChanged(IGASState<VS, ES, ST> state, Value u);

    /**
     * 
     * @param state
     * @param u
     *            The vertex for which the scatter will being performed.
     * @param e
     *            The edge.
     */
    void scatter(IGASState<VS, ES, ST> state, IGASScheduler sch, Value u, Statement e);

    /**
     * Return <code>true</code> iff the algorithm should continue. This is
     * invoked after every iteration, once the new frontier has been computed
     * and {@link IGASState#round()} has been advanced. An implementation may
     * simply return <code>true</code>, in which case the algorithm will
     * continue IFF the current frontier is not empty.
     * <p>
     * Note: While this can be used to make custom decisions concerning the
     * halting criteria, it can also be used as an opportunity to handshake with
     * a custom {@link IGraphAccessor} in order to process a dynamic graph.
     * 
     * @param ctx
     *            The evaluation context.
     * 
     * @return <code>true</code> if the algorithm should continue (as long as
     *         the frontier is non-empty).
     */
    boolean nextRound(IGASContext<VS, ES, ST> ctx);

}