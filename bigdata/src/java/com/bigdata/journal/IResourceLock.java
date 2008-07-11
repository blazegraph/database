package com.bigdata.journal;

/**
     * A lock granted by an {@link IResourceLockManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IResourceLock {

        /**
         * Release the lock.
         */
        public void unlock();
        
//        /**
//         * The unique identifier for this lock.
//         */
//        public UUID uuid();

    }