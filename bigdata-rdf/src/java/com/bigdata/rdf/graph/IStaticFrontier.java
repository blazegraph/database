package com.bigdata.rdf.graph;

import java.util.Iterator;

import org.openrdf.model.Value;

/**
 * Interface abstracts the fixed frontier as known on entry into a new
 * round.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public interface IStaticFrontier extends Iterable<Value> {

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
     * Reset the frontier from the supplied vertices.
     * 
     * @param minCapacity
     *            The minimum capacity of the new frontier. (A minimum capacity
     *            is specified since many techniques to compact the frontier can
     *            only estimate the required capacity.)
     * @param ordered
     *            <code>true</code> iff the frontier is known to be ordered.
     * @param vertices
     *            The vertices in the new frontier.
     * 
     *            FIXME The MergeSortIterator and ordered frontiers should only
     *            be used when the graph is backed by indices. The cache
     *            efficiency concerns for ordered frontiers apply when accessing
     *            indices. However, we should not presump that order of
     *            traversal matters for other backends. For example, a backend
     *            based on hash collections over RDF Statement objects will not
     *            benefit from an ordering based on the RDF Value objects.
     */
    void resetFrontier(int minCapacity, boolean ordered, Iterator<Value> vertices);

}
