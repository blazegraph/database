/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Mar 12, 2008
 */

package com.bigdata.resources;

import com.bigdata.journal.AbstractTask;

/**
 * Abstract base class for tasks run during post-processing of a journal by the
 * {@link ResourceManager}. This mainly allows those tasks to be grouped
 * together within the class hierarchy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractResourceManagerTask extends AbstractTask {

//    /**
//     * Interface aligned with {@link CountDownLatch}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public interface ILatch {
//        
//        /**
//         * @see CountDownLatch#countDown()
//         */
//        public void countDown();
//
//        /**
//         * @see CountDownLatch#getCount()
//         */
//        public long getCount();
//        
//        /**
//         * @see CountDownLatch#await()
//         */
//        public void await() throws InterruptedException;
//        
//        /**
//         * @see CountDownLatch#await(long, TimeUnit)
//         */
//        public boolean await(long timeout,TimeUnit unit) throws InterruptedException;
//        
//    }
//    
//    /**
//     * A wrapper for a {@link CountDownLatch} where we do not know starting
//     * count until after we have to give everyone a reference to the latch. The
//     * inner {@link CountDownLatch} is late-bound after all the tasks have been
//     * created but before they are submitted for execution by the
//     * {@link ConcurrencyManager}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class LateBoundLatch implements ILatch {
//        
//        private CountDownLatch innerLatch;
//        
//        public void start(int n) {
//            
//            if(innerLatch!=null) throw new IllegalStateException();
//            
//            innerLatch = new CountDownLatch(n);
//            
//        }
//        
//        public void countDown(){
//            
//            if(innerLatch==null) throw new IllegalStateException();
//            
//            innerLatch.countDown();
//            
//        }
//
//        public long getCount() {
//
//            if(innerLatch==null) throw new IllegalStateException();
//            
//            return innerLatch.getCount();
//            
//        }
//
//        public void await() throws InterruptedException {
//            
//            if(innerLatch==null) throw new IllegalStateException();
//
//            innerLatch.await();
//            
//        }
//
//        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
//            
//            if(innerLatch==null) throw new IllegalStateException();
//
//            return innerLatch.await(timeout, unit);
//        
//        }
//
//        public String toString() {
//            
//            if(innerLatch==null) return super.toString()+"{NotInitialized}";
//
//            return innerLatch.toString();
//            
//        }
//        
//    }
//    
//    protected final ILatch latch;
    
    protected final ResourceManager resourceManager;
    
    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     * @param latch
     */
    public AbstractResourceManagerTask(ResourceManager resourceManager, long timestamp,
            String resource) {//, ILatch latch) {

        super(resourceManager.getConcurrencyManager(), timestamp, resource);

        this.resourceManager = resourceManager;
        
//        if (latch == null)
//            throw new IllegalArgumentException();
//
//        this.latch = latch;
        
    }

    /**
     * @param resourceManager
     * @param timestamp
     * @param resource
     * @param latch
     */
    public AbstractResourceManagerTask(ResourceManager resourceManager, long timestamp,
            String[] resource) { //, ILatch latch) {

        super(resourceManager.getConcurrencyManager(), timestamp, resource);

        this.resourceManager = resourceManager;

//        if (latch == null)
//            throw new IllegalArgumentException();
//
//        this.latch = latch;

    }

}
