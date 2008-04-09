package com.bigdata.rdf.store;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.spo.IChunkedIterator;

/**
 * Writes {@link Justification}s on the justification index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationWriter implements Callable<Long>{

    /**
     * The database on which to write the justifications.
     */
    private final AbstractTripleStore dst;

    /**
     * The source iterator.
     */
    private final IChunkedIterator<Justification> src;

    /**
     * The #of justifications that were written on the justifications index.
     */
    private final AtomicLong nwritten;
    
    /**
     * 
     * @param dst
     *            The database on which the statements will be written.
     * @param src
     *            The source iterator.
     * @param nwritten
     *            Incremented as a side-effect for each justification
     *            actually written on the justification index.
     */
    public JustificationWriter(AbstractTripleStore dst, IChunkedIterator<Justification> src, AtomicLong nwritten) {
    
        this.dst = dst;
        
        this.src = src;
        
        this.nwritten = nwritten;
        
    }
    
    /**
     * Write justifications on the justifications index.
     * 
     * @return The elapsed time.
     */
    public Long call() throws Exception {
        
        final long begin = System.currentTimeMillis();
        
        nwritten.addAndGet(dst.addJustifications(src));
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        return elapsed;

    }
    
}