package com.bigdata.rdf.graph;

import com.bigdata.rdf.internal.IV;

/**
 * Extended {@link IGASScheduler} interface. This interface is exposed to the
 * implementation of the GAS Engine. The methods on this interface are NOT for
 * use by the {@link IGASProgram} and MIGHT NOT (really, should not) be
 * available on the {@link IGASScheduler} supplied to an {@link IGASProgram}.
 * 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASSchedulerImpl extends IGASScheduler {

    /**
     * Compact the schedule into the new frontier.
     * <p>
     * Note: Typical contracts ensure that the frontier is compact (no
     * duplicates) and in ascending {@link IV} order (this provides cache
     * locality for the index reads, even if those reads are against indices
     * wired into RAM).
     */
    void compactFrontier(IStaticFrontier frontier);

    /**
     * Reset all internal state (and get rid of any thread locals).
     */
    void clear();

}
