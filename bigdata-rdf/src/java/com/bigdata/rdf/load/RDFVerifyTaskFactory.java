package com.bigdata.rdf.load;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Factory for tasks for verifying a database against RDF resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFVerifyTaskFactory<T extends Runnable> extends
        AbstractRDFTaskFactory<T> {

    public RDFVerifyTaskFactory(AbstractTripleStore db, int bufferCapacity,
            boolean verifyData, boolean deleteAfter, RDFFormat fallback) {

        super(db, verifyData, deleteAfter, fallback, new VerifyStatementBufferFactory(
                db, bufferCapacity));

    }

    public long getTermCount() {
        
        return ((VerifyStatementBufferFactory)bufferFactory).nterms.get();
        
    }
    
    public long getTermNotFoundCount() {
        
        return ((VerifyStatementBufferFactory)bufferFactory).ntermsNotFound.get();
        
    }
    
    public long getTripleCount() {
        
        return ((VerifyStatementBufferFactory)bufferFactory).ntriples.get();
        
    }
    
    public long getTripleNotFoundCount() {
        
        return ((VerifyStatementBufferFactory)bufferFactory).ntriplesNotFound.get();
        
    }
    
    /**
     * Report on #terms and #stmts not found as well as #triples processed
     * and found.
     */
    public String reportTotals() {

        // total run time.
        final long elapsed = elapsed();

        final long tripleCount = getTripleCount();

        final double tps = (long) (((double) tripleCount) / ((double) elapsed) * 1000d);

        return "Processed: #terms=" + getTermCount() + " ("
                + getTermNotFoundCount() + " not found), #stmts="
                + tripleCount + " (" + getTripleNotFoundCount()
                + " not found)" + ", rate=" + tps + " in " + elapsed
                + " ms.";
            
    }
    
}