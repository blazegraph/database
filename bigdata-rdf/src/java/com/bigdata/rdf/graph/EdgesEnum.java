package com.bigdata.rdf.graph;

/**
 * Typesafe enumeration used to specify whether a GATHER or SCATTER phase is
 * applied to the in-edges, out-edges, both, or not run.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public enum EdgesEnum {
    
    /** The phase is not run. */
    NoEdges(false/* inEdges */, false/* outEdges */),
    
    /** The phase is applied only to the in-edges. */
    InEdges(true/* inEdges */, false/* outEdges */),
    
    /** The phase is applied only to the out-edges. */
    OutEdges(false/* inEdges */, true/* outEdges */),
    
    /** The phase is applied to all edges (both in-edges and out-edges). */
    AllEdges(true/* inEdges */, true/* outEdges */);

    private EdgesEnum(final boolean inEdges, final boolean outEdges) {
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }
    
    private final boolean inEdges;
    private final boolean outEdges;
    
    /**
     * Return <code>true</code>iff the in-edges will be visited.
     */
    public boolean doInEdges() {
        return inEdges;
    }

    /**
     * Return <code>true</code>iff the out-edges will be visited.
     */
    public boolean doOutEdges() {
        return outEdges;
    }

    /**
     * Return <code>true</code> iff the value is either {@link #InEdges} or
     * {@link #OutEdges}.
     */
    public boolean isDirected() {
        switch (this) {
        case NoEdges:
            /*
             * Note: None is neither directed nor non-directed regardless of the
             * value returned here. The caller should not be inquiring about
             * directedness of the GATHER or SCATTER unless they will be
             * executing that stage.
             */
            return false;
        case AllEdges:
            return false;
        case InEdges:
            return true;
        case OutEdges:
            return true;
        default:
            throw new UnsupportedOperationException();
        }
    }

}