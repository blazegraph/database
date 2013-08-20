package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
@SuppressWarnings("rawtypes")
public interface IGASProgram<VS, ES, ST> extends IGASOptions<VS, ES> {

    /*
     * TODO Unfortunately this pattern of hiding our more complex interfaces can
     * not be made to work without creating wrapper objects that implement the
     * derived interface, even though we would just like to use it as a marker
     * interface. It might be workable if we put this under the IV and ISPO
     * interfaces (as simpler interfaces without generic types).
     */
    
//    /**
//     * A shorthand for the {@link IV} interface that cleans up the generic type
//     * warnings. An {@link IV} corresponds to a vertex of the graph or the value
//     * of an attribute. {@link IV}s may be materialized or not. For efficiency,
//     * it is better to operate without materialization of the corresponding RDF
//     * {@link Value}. Many {@link IV}s are <em>inline</em> can be immediately
//     * interpreted as if they were materialized RDF {@link Value}s - for
//     * example, this is true by default for all <code>xsd</code> numeric
//     * datatypes. It may also be true of other kinds of {@link Value}s depending
//     * on how the KB was configured.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    @SuppressWarnings("rawtypes")
//    private interface IV extends com.bigdata.rdf.internal.IV {
//        
//    }
//
//    /**
//     * An edge is comprised of a Subject (s), Predicate (p), and Object (o).
//     * Depending on the KB configuration, there may also be an Context (c)
//     * position on the edge - when present the Context supports the concept of
//     * SPARQL named graphs.
//     * <dl>
//     * <dt>Subject</dt>
//     * <dd>The Subject is either a {@link URI} or a {@link BNode}.</dd>
//     * <dt>Predicate</dt>
//     * <dd>The Predicate is always a {@link URI}.</dd>
//     * <dt>Object</dt>
//     * <dd>The Object is either a {@link URI} (in which case the "edge" is a
//     * link) or a {@link Literal} (in which case the edge is a property value).</dd>
//     * <dt>Context</dt>
//     * <dd>The Context is either a {@link URI} or a {@link BNode}.</dd>
//     * </dl>
//     * Note that the Subject, Predicate, Object, and Context will be {@link IV}
//     * instances and hence might or might not be materialized RDF {@link Value}s
//     * and might or might not be <em>inline</em> and hence directly inspectable
//     * as if they were materialized RDF {@link Value}s.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    private interface ISPO extends com.bigdata.rdf.spo.ISPO {
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        IV s();
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        IV p();
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        IV o();
//
//        /**
//         * {@inheritDoc}
//         */
//        @Override
//        IV c();
//        
//    }

    /**
     * Callback to initialize the state for each vertex in the initial frontier
     * before the first iteration. A typical use case is to set the distance of
     * the starting vertex to ZERO (0).
     * 
     * @param u
     *            The vertex.
     */
    void init(IGASContext<VS, ES, ST> ctx, IV u);
    
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
     *         not need.
     *         <p>
     *         Note: However, this might cause problems with a powergraph style
     *         decomposition onto a cluster since the state needs to be
     *         communicated up front if it will be required by the gather() for
     *         the edge.
     *         <p>
     *         Note: The state associated with the source/target vertex and the
     *         edge should all be immutable for the GATHER. The vertex state
     *         should only be mutable for the APPLY(). The target vertex state
     *         and/or edge state MAY be mutable for the SCATTER, but that
     *         depends on the algorithm. How can we get these constraints into
     *         the API?
     * 
     *         TODO If gather/scatter over ALL edges, then do we need to pass
     *         through a parameter so the caller can figure out what direction
     *         the edge points in (alternatively, pass in the vertex for which
     *         the gather is being performance and they can reference test both
     *         [s] and [o] to see which one is the vertex on which the gather is
     *         invoked and which one is the remote vertex.
     */
    ST gather(IGASContext<VS, ES, ST> ctx, IV u, ISPO e);
    
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
     */
    ST sum(ST left, ST right);

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
    VS apply(IGASContext<VS, ES, ST> ctx, IV u, ST sum);

    /**
     * Return <code>true</code> iff the vertex should run its SCATTER phase.
     * This may be used to avoid visiting the edges if it is known (e.g., based
     * on the APPLY) that the vertex has not changed. This can save a
     * substantial amount of effort.
     * 
     * @param ctx
     * @param u
     *            The vertex.
     * @return
     */
    boolean isChanged(IGASContext<VS, ES, ST> ctx, IV u);

    /**
     * 
     * @param ctx
     * @param u
     *            The vertex for which the scatter will being performed.
     * @param e
     *            The edge.
     */
    void scatter(IGASContext<VS, ES, ST> ctx, IV u, ISPO e);
     
}