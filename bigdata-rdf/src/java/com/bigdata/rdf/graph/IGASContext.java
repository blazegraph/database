package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Execution context for an {@link IGASProgram}. This is distinct from the
 * {@link IGASEngine} so we can support distributed evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
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
@SuppressWarnings("rawtypes")
public interface IGASContext<VS, ES, ST> {

    /**
     * Schedule a vertex for execution.
     * 
     * @param v
     *            The vertex.
     */
    void schedule(IV v);
 
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
    VS getState(IV v);

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
     * Compute a reduction over the vertex state table (all vertices that have
     * had their vertex state materialized).
     * 
     * @param op
     *            The reduction operation.
     * 
     * @return The reduction.
     */
    <T> T reduce(IReducer<VS, ES, ST, T> op);
    
}