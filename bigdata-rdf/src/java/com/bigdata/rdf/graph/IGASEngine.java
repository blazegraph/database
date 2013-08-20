package com.bigdata.rdf.graph;

import java.util.concurrent.Callable;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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
 *            FIXME This should be refactored to allow a singleton for the
 *            {@link IGASEngine} (for a server process, much like a QueryEngine)
 *            and then to create an {@link IGASContext} to execute an
 *            {@link IGASProgram}. This would allow us to reuse resources within
 *            the {@link IGASEngine}.
 */
public interface IGASEngine<VS, ES, ST> extends Callable<Void> {

    /**
     * Return the graph.
     */
    AbstractTripleStore getKB();

    /**
     * Return the program that is being evaluated.
     */
    IGASProgram<VS, ES, ST> getGASProgram();
    
    /**
     * The execution context for the {@link IGASEngine}.
     */
    IGASContext<VS, ES, ST> getGASContext();
    
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
     * Execute one iteration.
     * 
     * @return true iff the new frontier is empty.
     */
    boolean doRound();
    
}