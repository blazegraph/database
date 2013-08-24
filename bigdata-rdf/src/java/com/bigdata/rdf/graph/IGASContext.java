package com.bigdata.rdf.graph;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Execution context for an {@link IGASProgram}. This is distinct from the
 * {@link IGASEngine} so we can support distributed evaluation and concurrent
 * evaluation of multiple {@link IGASProgram}s.
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
public interface IGASContext<VS, ES, ST> extends Callable<IGASStats> {

    /**
     * Return the program that is being evaluated.
     */
    IGASProgram<VS, ES, ST> getGASProgram();

    /**
     * The computation state.
     */
    IGASState<VS, ES, ST> getGASState();
    
    /**
     * Execute one iteration.
     * 
     * @param stats
     *            Used to report statistics about the execution of the
     *            algorithm.
     * 
     * @return true iff the new frontier is empty.
     */
    boolean doRound(IGASStats stats) throws Exception, ExecutionException,
            InterruptedException;

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