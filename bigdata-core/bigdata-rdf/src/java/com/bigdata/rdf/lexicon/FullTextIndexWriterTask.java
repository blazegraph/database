package com.bigdata.rdf.lexicon;

import java.util.Iterator;
import java.util.concurrent.Callable;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Index terms for keyword search.
 */
class FullTextIndexWriterTask implements Callable<Long> {

//    static private final transient Logger log = Logger
//            .getLogger(FullTextIndexWriterTask.class);

    private final IValueCentricTextIndexer<?> textIndexer;
    
    private final int capacity;
    
    private final Iterator<BigdataValue> itr;

    public FullTextIndexWriterTask(final IValueCentricTextIndexer<?> textIndexer,
            final int capacity, final Iterator<BigdataValue> itr) {

        if (textIndexer == null)
            throw new IllegalArgumentException();

        this.textIndexer = textIndexer;
        
        this.capacity = capacity;
        
        this.itr = itr;
        
    }
    
    /**
     * Elapsed time for this operation.
     */
    public Long call() throws Exception {

        final long _begin = System.currentTimeMillis();

        textIndexer.index(capacity, itr);

        final long elapsed = System.currentTimeMillis() - _begin;
        
        return elapsed;

    }
    
}