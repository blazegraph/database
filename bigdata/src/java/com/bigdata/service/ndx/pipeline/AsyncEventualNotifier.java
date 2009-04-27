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
 * Created on Apr 22, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;

import com.bigdata.btree.keys.KVO;
import com.bigdata.cache.ConcurrentWeakValueCache;

/**
 * This provides an "eventual" notification mechanism for asynchronous writes
 * which allows you to detect when all tuples for some user-defined scope have
 * been cleared from the pipeline (made restart safe on the database).
 * <p>
 * The notification is based on a weak value hash map. The keys of the map are
 * whatever notice you want to receive, e.g., the name of a file whose contents
 * have been fully processed. The weak value reference is an application
 * generated object that is unique to the scope of the processing. You MUST NOT
 * hold a hard reference to this object!!! Notices are generated when the weak
 * value reference is cleared. The weak value reference is attached to
 * {@link KVORef} instance for the processing scope for which you want to be
 * notified. When all {@link KVORef}s with a given reference have been
 * processed, the garbage collector will sweep the reference and the weak value
 * hash map will notice the cleared reference.
 * <P>
 * For any significant workload, notification should be quite fast. However, if
 * GC is not being driven by heap churn then notification may not occur. In
 * practice, this is mainly an issue when writing unit tests since you can not
 * force the JVM to clear the weak references.
 * <p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AsyncEventualNotifier<K,R> {

    static public interface IAsyncEventualListener<K> {
        
        /**
         * Invoked when the weak reference for the key was cleared.
         * 
         * @param key
         *            The key.
         */
        public void referenceCleared(K key);
        
    }
    
    /**
     * A weak value hash map suitable for concurrent operations with an EMPTY
     * hard reference queue.
     * 
     * @todo impl does not allow the hard reference queue to be empty.
     */
    private final ConcurrentWeakValueCache<K, R> map;
    
    private final IAsyncEventualListener<K> listener;
    
    public AsyncEventualNotifier(final IAsyncEventualListener<K> listener) {

        if (listener == null)
            throw new IllegalArgumentException();
        
        this.listener = listener;
        
        map = new ConcurrentWeakValueCache<K, R>(0/* queueCapacity */) {

            @Override
            protected WeakReference<R> removeMapEntry(final K k) {
                
                try {
                
                    // notify the listener.
                    AsyncEventualNotifier.this.listener.referenceCleared(k);
                
                } catch(Throwable t) {
                    
                    // ignore all errors thrown by the listener.
                    log.warn("Listener: "+t,t);
                    
                }
                
                return super.removeMapEntry(k);
                
            }
            
        };
        
    }
    
    public static class KVORef<O> extends KVO<O> {

        private final Object ref;
        
        /**
         * 
         * @param key
         *            The unsigned byte[] key (required).
         * @param val
         *            The byte[] value (optional).
         * @param obj
         *            The paired application object (optional).
         * @param ref
         *            A reference entered into a weak value hash map (required).
         */
        public KVORef(final byte[] key, final byte[] val, final O obj,
                final Object ref) {

            super(key, val, obj);

            this.ref = ref;

        }

    }

}
