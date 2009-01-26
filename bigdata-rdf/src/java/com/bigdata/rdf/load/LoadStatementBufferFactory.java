package com.bigdata.rdf.load;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStatementBufferFactory implements IStatementBufferFactory {

    private final AbstractTripleStore db;

    private final int bufferCapacity;

    public LoadStatementBufferFactory(AbstractTripleStore db,
            int bufferCapacity) {

        this.db = db;
       
        this.bufferCapacity = bufferCapacity;
        
    }
    
    /**
     * Return the {@link ThreadLocal} {@link StatementBuffer} to be used for a
     * task.
     */
    public StatementBuffer getStatementBuffer() {

        /*
         * Note: this is a thread-local so the same buffer object is always
         * reused by the same thread.
         */

        return threadLocal.get();
        
    }
    
    private ThreadLocal<StatementBuffer> threadLocal = new ThreadLocal<StatementBuffer>() {

        protected synchronized StatementBuffer initialValue() {
            
            return new StatementBuffer(db, bufferCapacity);
            
        }

    };
    
}