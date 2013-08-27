package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

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
     * @param v
     *            One or more vertices that will be included in the initial
     *            frontier.
     * 
     * @throws IllegalArgumentException
     *             if no vertices are specified.
     */
    void init(@SuppressWarnings("rawtypes") IV... v);

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
    VS getState(@SuppressWarnings("rawtypes") IV v);

    /**
     * Get the state for the edge using the appropriate factory. If this is the
     * first visit for that edge, then the state is initialized using the
     * factory. Otherwise the existing state is returned.
     * 
     * @param v
     *            The vertex.
     * 
     * @return The state for that vertex.
     * 
     * @see IGASProgram#getEdgeStateFactory()
     */
    ES getState(ISPO e);

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
     * Return a useful representation of an edge (non-batch API, debug only).
     * 
     * @param e
     *            The edge.
     * @return The representation of that edge.
     */
    String toString(ISPO e);

}
