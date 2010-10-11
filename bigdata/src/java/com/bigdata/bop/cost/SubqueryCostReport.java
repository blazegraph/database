package com.bigdata.bop.cost;

import java.io.Serializable;

/**
 * Subquery cost report.
 */
public class SubqueryCostReport implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** The #of graphs against which subqueries will be issued. */
    public final int ngraphs;

    /** The #of samples to be taken. */
    public final int limit;

    /** The #of samples taken. */
    public final int nsamples;

    /**
     * An estimated range count based on the samples and adjusted for the
     * #of graphs.
     */
    public final long rangeCount;

    /**
     * An estimated cost (latency in milliseconds) based on the samples and
     * adjusted for the #of graphs.
     */
    public final double cost;

    /**
     * 
     * @param ngraphs
     *            The #of graphs against which subqueries will be issued.
     * @param limit
     *            The #of samples to be taken.
     * @param nsamples
     *            The #of samples taken.
     * @param rangeCount
     *            An estimated range count based on the samples and adjusted
     *            for the #of graphs.
     * @param cost
     *            An estimated cost (latency in milliseconds) based on the
     *            samples and adjusted for the #of graphs.
     */
    public SubqueryCostReport(final int ngraphs, final int limit,
            final int nsamples, final long rangeCount, final double cost) {

        if (ngraphs < 0)
            throw new IllegalArgumentException();
        
        if (limit < 1)
            throw new IllegalArgumentException();
        
        if (nsamples < 0)
            throw new IllegalArgumentException();
        
        if (rangeCount < 0)
            throw new IllegalArgumentException();
        
        if (cost < 0)
            throw new IllegalArgumentException();

        this.ngraphs = ngraphs;
        
        this.limit = limit;
        
        this.nsamples = nsamples;
        
        this.rangeCount = rangeCount;
        
        this.cost = cost;
    
    }
    
    /**
     * Human readable representation.
     */
    public String toString() {
        return super.toString() + //
                "{ngraphs=" + ngraphs + //
                ",limit=" + limit + //
                ",nsamples=" + nsamples + //
                ",rangeCount=" + rangeCount + //
                ",cost=" + cost + //
                "}";
    }
    
}
