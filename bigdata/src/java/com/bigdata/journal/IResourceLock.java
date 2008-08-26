package com.bigdata.journal;

import java.io.IOException;
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
    public void unlock() throws IOException;

    //        /**
    //         * The unique identifier for this lock.
    //         */
    //        public UUID uuid();

}
