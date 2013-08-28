package com.bigdata.rdf.graph;

import java.util.Iterator;

import com.bigdata.rdf.internal.IV;

/**
 * Interface abstracts the fixed frontier as known on entry into a new
 * round.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
@SuppressWarnings("rawtypes")
public interface IStaticFrontier extends Iterable<IV> {

    /**
     * The number of vertices in the frontier.
     * 
     * TODO Long? Or just do not allow in scale-out?
     */
    int size();

    /**
     * Return <code>true</code> if the frontier is known to be empty.
     */
    boolean isEmpty();
    
    /**
     * Return <code>true</code> iff the frontier is known to be compact (no
     * duplicate vertices).
     * <p>
     * Note: If the frontier is not compact, then the {@link IGASEngine} may
     * optionally elect to eliminate duplicate work when it schedules the
     * vertices in the frontier.
     * <p>
     * Note: A non-compact frontier can arise when the {@link IGASScheduler}
     * chooses a per-thread approach and then copies the per-thread segments
     * onto the shared backing array in parallel. This can reduce the time
     * between rounds, which can speed up the overall execution of the algorithm
     * significantly.
     */
    boolean isCompact();

    /**
     * Reset the frontier from the {@link IV}s.
     * 
     * @param minCapacity
     *            The minimum capacity of the new frontier. (A minimum capacity
     *            is specified since many techniques to compact the frontier can
     *            only estimate the required capacity.)
     * @param ordered
     *            <code>true</code> iff the frontier is known to be ordered.
     * @param vertices
     *            The vertices in the new frontier.
     */
    void resetFrontier(int minCapacity, boolean ordered, Iterator<IV> vertices);

}
