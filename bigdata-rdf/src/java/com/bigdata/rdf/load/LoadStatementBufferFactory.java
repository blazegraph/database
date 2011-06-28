package com.bigdata.rdf.load;

import org.openrdf.model.Statement;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStatementBufferFactory<S extends Statement> implements
        IStatementBufferFactory<S> {

    private final AbstractTripleStore db;

    private final int bufferCapacity;

    /**
     * 
     * @param db
     * @param bufferCapacity
     */
    public LoadStatementBufferFactory(final AbstractTripleStore db,
            final int bufferCapacity) {

        this.db = db;

        this.bufferCapacity = bufferCapacity;
    
    }
    
    /**
     * Return the {@link StatementBuffer} to be used for a task.
     */
    public StatementBuffer<S> newStatementBuffer() {

        return new StatementBuffer<S>(null/* statementStore */, db,
                bufferCapacity);

    }
    
//    /**
//     * Return the {@link ThreadLocal} {@link StatementBuffer} to be used for a
//     * task.
//     */
//    public StatementBuffer<S> newStatementBuffer() {
//
//        /*
//         * Note: this is a thread-local so the same buffer object is always
//         * reused by the same thread.
//         */
//
//        return threadLocal.get();
//        
//    }
//
//    private ThreadLocal<StatementBuffer<S>> threadLocal = new ThreadLocal<StatementBuffer<S>>() {
//
//        protected synchronized StatementBuffer<S> initialValue() {
//
//            return new StatementBuffer<S>(null/* statementStore */, db,
//                    bufferCapacity, writeBuffer);
//            
//        }
//
//    };
    
}
