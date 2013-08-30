package com.bigdata.rdf.graph;

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
     */
    void compactFrontier(IStaticFrontier frontier);

    /**
     * Reset all internal state (and get rid of any thread locals).
     */
    void clear();

}
