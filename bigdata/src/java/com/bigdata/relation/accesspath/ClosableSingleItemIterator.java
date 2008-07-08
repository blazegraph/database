package com.bigdata.relation.accesspath;

import java.util.NoSuchElementException;


/**
 * A closable iterator that visits a single item.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class ClosableSingleItemIterator<E> implements IClosableIterator<E> {

    private boolean done = false;
    
    private final E element;
    
    public ClosableSingleItemIterator(E element) {
        
        this.element = element;
        
    }
    
    public void close() {
        // NOP
    }

    public boolean hasNext() {
    
        return !done;
        
    }

    public E next() {
        
        if(!hasNext()) throw new NoSuchElementException();
        
        done = true;
        
        return element;
    }

    public void remove() {

        throw new UnsupportedOperationException();
        
    }
    
}