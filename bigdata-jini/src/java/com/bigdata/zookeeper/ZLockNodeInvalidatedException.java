package com.bigdata.zookeeper;

/**
 * This exception is thrown if there is an attempt to acquire a {@link ZLock}
 * but the lock node has been invalidated pending its destruction. This is
 * essentially a distributed "interrupt". The lock WILL NOT be granted and the
 * lock node itself should disappear shortly.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZLockNodeInvalidatedException extends InterruptedException {

    /**
     * 
     */
    private static final long serialVersionUID = 2491240005134272857L;

    public ZLockNodeInvalidatedException(String zpath) {
        
        super(zpath);
        
    }
    
}