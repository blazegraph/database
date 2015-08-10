package com.bigdata.journal;

import java.rmi.Remote;

/**
 * A lock granted by an {@link IResourceLockService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IResourceLock extends Remote {

    /**
     * Release the lock.
     */
    public void unlock();

}
