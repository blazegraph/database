package com.bigdata.rdf.sail;

import java.util.NoSuchElementException;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.sail.SailException;

/**
 * Class exists to align exceptions thrown by Sesame 2 query evaluation with
 * those thrown by the Sesame 2 SAIL.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryEvaluationIterator<T> implements
        CloseableIteration<T, QueryEvaluationException> {

    private final CloseableIteration<? extends T, SailException> src;

    private boolean open = true;

    public QueryEvaluationIterator(
            CloseableIteration<? extends T, SailException> src) {

        assert src != null;
        
        this.src = src;
        
    }
    
    public boolean hasNext() throws QueryEvaluationException {
        
        if(open && _hasNext())
            return true;
        
        close();
        
        return false;
        
    }

    private boolean _hasNext() throws QueryEvaluationException {
        
        try {

            return src.hasNext();
            
        } catch (SailException ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
    }

    public T next() throws QueryEvaluationException {

        if(!hasNext())
            throw new NoSuchElementException();

        try {

            return (T) src.next();
            
        } catch(SailException ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
    }

    public void remove() throws QueryEvaluationException {

        if (!open)
            throw new IllegalStateException();

        try {

            src.remove();
            
        } catch(SailException ex) {
            
            throw new QueryEvaluationException(ex);
            
        }
        
    }
    
    public void close() throws QueryEvaluationException {

        if (open) {

            open = false;

            try {

                src.close();

            } catch (SailException ex) {

                throw new QueryEvaluationException(ex);

            }

        }
                
    }

}
