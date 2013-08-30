package com.bigdata.rdf.graph;

/**
 * Statistics for GAS algorithms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASStats {

    void add(final long frontierSize, final long nedges, final long elapsedNanos);

    void add(final IGASStats o);

    long getNRounds();

    /**
     * The cumulative size of the frontier across the iterations.
     */
    long getFrontierSize();

    /**
     * The number of traversed edges across the iterations.
     */
    long getNEdges();

    /**
     * The elapsed nanoseconds across the iterations.
     */
    long getElapsedNanos();

}
