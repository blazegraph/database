package com.bigdata.rdf.load;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStatementBufferFactory implements IStatementBufferFactory {

    private final AbstractTripleStore db;

    private final int bufferCapacity;

    private final BlockingBuffer<ISPO[]> writeBuffer;
    
    /**
     * 
     * @param db
     * @param bufferCapacity
     * @param writeBuffer
     *            An optional buffer for asynchronous writes on the statement
     *            indices.
     * 
     * @todo drop the writeBuffer arg.
     */
    public LoadStatementBufferFactory(final AbstractTripleStore db,
            final int bufferCapacity, final BlockingBuffer<ISPO[]> writeBuffer) {

        this.db = db;
       
        this.bufferCapacity = bufferCapacity;
    
        this.writeBuffer = writeBuffer;
        
    }
    
    /**
     * Return the {@link ThreadLocal} {@link StatementBuffer} to be used for a
     * task.
     */
    public StatementBuffer newStatementBuffer() {

        /*
         * Note: this is a thread-local so the same buffer object is always
         * reused by the same thread.
         */

        return threadLocal.get();
        
    }

    private ThreadLocal<StatementBuffer> threadLocal = new ThreadLocal<StatementBuffer>() {

        protected synchronized StatementBuffer initialValue() {

            return new StatementBuffer(null/* statementStore */, db,
                    bufferCapacity, writeBuffer);
            
        }

    };
    
}