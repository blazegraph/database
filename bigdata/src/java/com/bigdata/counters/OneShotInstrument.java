package com.bigdata.counters;

/**
 * An {@link Instrument} that records a single value at the moment that it
 * is constructed and always reports the same value and lastModified time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class OneShotInstrument<T> implements IInstrument<T> {
    
    private final T value;
    private long lastModified = System.currentTimeMillis();
    
    public OneShotInstrument(T value) {

        this.value = value;

    }

    public T getValue() {
        
        return value;
        
    }

    public long lastModified() {
 
        return lastModified;
        
    }

    /**
     * @throws UnsupportedOperationException
     *             always
     */
    public void setValue(T value, long timestamp) {

        throw new UnsupportedOperationException();
        
    }

}