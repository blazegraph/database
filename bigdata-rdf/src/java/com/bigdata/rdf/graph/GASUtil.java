package com.bigdata.rdf.graph;


import java.util.concurrent.TimeUnit;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;

/**
 * Utility class for operations on the public interfaces.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASUtil {

//    private static final Logger log = Logger.getLogger(GASUtil.class);

    /**
     * Return the other end of a link.
     * 
     * @param u
     *            One end of the link.
     * @param e
     *            The link.
     * 
     * @return The other end of the link.
     * 
     *         FIXME We can optimize this to use reference testing if we are
     *         careful in the GATHER and SCATTER implementations to always use
     *         the {@link IV} values on the {@link ISPO} object that is exposed
     *         to the {@link IGASProgram}.
     */
    @SuppressWarnings("rawtypes")
    public static IV getOtherVertex(final IV u, final ISPO e) {

        if (e.s().equals(u))
            return e.o();
        
        return e.s();

    }

    /**
     * The average fan out of the frontier.
     * 
     * @param frontierSize
     *            The size of the frontier.
     * @param nedges
     *            The number of edges visited when mapping the operation across
     *            that frontier.
     * 
     * @return The average fan out.
     */
    public static double fanOut(final int frontierSize, final long nedges) {
    
        return ((int) (nedges * 10d / frontierSize)) / 10d;
    
    }

    /**
     * The traversed edges per second.
     * 
     * @param nedges
     *            The number of traversed edges.
     * @param elapsedNanos
     *            The elapsed time (nanoseconds).
     *            
     * @return The traversed edges per second.
     */
    public static long getTEPS(final long nedges, long elapsedNanos) {

        // avoid division by zero.
        if (elapsedNanos == 0)
            elapsedNanos = 1;

        // edges/nanosecond.
        final double tepns = ((double) nedges) / elapsedNanos;

        // scale units to edges/second.
        final double teps = tepns * TimeUnit.SECONDS.toNanos(1);

        // Round off to get rid of those nasty factions.
        final long r = Math.round(teps);

        return r;

    }

}
