package com.bigdata.join;

import java.util.concurrent.Callable;

/**
 * Task invokes {@link IBuffer#flush()} and returns its return value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FlushBufferTask implements Callable<Long> {
    
    final private IBuffer buffer;
    
    public FlushBufferTask(IBuffer buffer) {

        if (buffer == null)
            throw new IllegalArgumentException();

        this.buffer = buffer;
        
    }
    
    /**
     * @return The mutation count from {@link IBuffer#flush()}.
     */
    public Long call() {
        
        return buffer.flush();
        
    }
    
}