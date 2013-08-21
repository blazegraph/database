package com.bigdata.rdf.graph.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASStats;

/**
 * FIXME Refactor to a pure interface - see RuleStats.
 * 
 * FIXME Collect the details within round statistics and then lift the
 * formatting of the statistics into this class (for the details within round
 * statistics).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASStats implements IGASStats {

    private final AtomicLong nrounds = new AtomicLong();
    private final AtomicLong frontierSize = new AtomicLong();
    private final AtomicLong nedges = new AtomicLong();
    private final AtomicLong elapsedNanos = new AtomicLong();

    public void add(final long frontierSize, final long nedges,
            final long elapsedNanos) {

        this.nrounds.incrementAndGet();
        
        this.frontierSize.addAndGet(frontierSize);

        this.nedges.addAndGet(nedges);

        this.elapsedNanos.addAndGet(elapsedNanos);

    }

    public void add(final GASStats o) {

        nrounds.addAndGet(o.getNRounds());
        
        frontierSize.addAndGet(o.getFrontierSize());
        
        nedges.addAndGet(o.getNEdges());

        elapsedNanos.addAndGet(o.getElapsedNanos());
        
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
                + ": fontierSize=" + getFrontierSize() //
                + ", ms=" + TimeUnit.NANOSECONDS.toMillis(getElapsedNanos())//
                + ", edges=" + getNEdges()//
                + ", teps=" + GASUtil.getTEPS(getNEdges(), getElapsedNanos())//
        ;
    }
    
}
