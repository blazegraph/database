package com.bigdata.rdf.graph.impl;

import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CAT;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASStats;

/**
 * FIXME STATS: Refactor to a pure interface - see RuleStats.
 * 
 * FIXME STATS: Collect the details within round statistics and then lift the
 * formatting of the statistics into this class (for the details within round
 * statistics).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASStats implements IGASStats {

    private final CAT nrounds = new CAT();
    private final CAT frontierSize = new CAT();
    private final CAT nedges = new CAT();
    private final CAT elapsedNanos = new CAT();

    public void add(final long frontierSize, final long nedges,
            final long elapsedNanos) {

        this.nrounds.increment();
        
        this.frontierSize.add(frontierSize);

        this.nedges.add(nedges);

        this.elapsedNanos.add(elapsedNanos);

    }

    public void add(final GASStats o) {

        nrounds.add(o.getNRounds());
        
        frontierSize.add(o.getFrontierSize());
        
        nedges.add(o.getNEdges());

        elapsedNanos.add(o.getElapsedNanos());
        
    }

    public long getNRounds() {
        return nrounds.get();
    }
    
    /**
     * The cumulative size of the frontier across the iterations.
     */
    public long getFrontierSize() {
        return frontierSize.get();
    }

    /**
     * The number of traversed edges across the iterations.
     */
    public long getNEdges() {
        return nedges.get();
    }

    /**
     * The elapsed nanoseconds across the iterations.
     */
    public long getElapsedNanos() {
        return elapsedNanos.get();
    }

    /**
     * Return a useful summary of the collected statistics.
     */
    @Override
    public String toString() {

        return "nrounds=" + getNRounds()//
                + ", fontierSize=" + getFrontierSize() //
                + ", ms=" + TimeUnit.NANOSECONDS.toMillis(getElapsedNanos())//
                + ", edges=" + getNEdges()//
                + ", teps=" + GASUtil.getTEPS(getNEdges(), getElapsedNanos())//
        ;
    }
    
}
