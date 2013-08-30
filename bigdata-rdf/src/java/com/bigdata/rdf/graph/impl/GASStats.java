package com.bigdata.rdf.graph.impl;

import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CAT;
import com.bigdata.rdf.graph.GASUtil;
import com.bigdata.rdf.graph.IGASStats;

/**
 * Statistics for GAS algorithms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASStats implements IGASStats {

    private final CAT nrounds = new CAT();
    private final CAT frontierSize = new CAT();
    private final CAT nedges = new CAT();
    private final CAT elapsedNanos = new CAT();

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#add(long, long, long)
     */
    @Override
    public void add(final long frontierSize, final long nedges,
            final long elapsedNanos) {

        this.nrounds.increment();
        
        this.frontierSize.add(frontierSize);

        this.nedges.add(nedges);

        this.elapsedNanos.add(elapsedNanos);

    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#add(com.bigdata.rdf.graph.impl.IFOO)
     */
    @Override
    public void add(final IGASStats o) {

        nrounds.add(o.getNRounds());
        
        frontierSize.add(o.getFrontierSize());
        
        nedges.add(o.getNEdges());

        elapsedNanos.add(o.getElapsedNanos());
        
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getNRounds()
     */
    @Override
    public long getNRounds() {
        return nrounds.get();
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getFrontierSize()
     */
    @Override
    public long getFrontierSize() {
        return frontierSize.get();
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getNEdges()
     */
    @Override
    public long getNEdges() {
        return nedges.get();
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getElapsedNanos()
     */
    @Override
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
