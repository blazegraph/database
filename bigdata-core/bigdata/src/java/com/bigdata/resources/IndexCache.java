/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Apr 11, 2012
 */

package com.bigdata.resources;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.journal.ITx;
import com.bigdata.util.NT;

/**
 * Extends the {@link ConcurrentWeakValueCache} to track the earliest timestamp
 * from which any local {@link IIndex} view is reading. This timestamp is
 * reported by {@link #getRetentionTime()}. The {@link StoreManager} uses this
 * in {@link StoreManager#purgeOldResources()} to provide a "read lock" such
 * that resources for in use views are not released.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexCache<H extends ILocalBTreeView> extends
        ConcurrentWeakValueCacheWithTimeout<NT, H> {

    // @todo remove debug flag?
    private static final boolean debug = false;
    
    /**
     * The earliest timestamp that must be retained for the read-historical
     * indices in the cache, {@link Long#MAX_VALUE} if there a NO
     * read-historical indices in the cache, and (-1L) if the value is not
     * known and must be computed by scanning the index cache.
     * 
     * @todo an alternative is to maintain a sorted set of the timestamps
     *       (breaking ties with the index name) and then always return the
     *       first entry (lowest value) from the set. in fact the tx service
     *       does something similar, but it has an easier time since the
     *       timestamps are unique where the same timestamp may be used for
     *       multiple indices here.
     */
    final private AtomicLong retentionTime = new AtomicLong(-1L);

    /**
     * 
     * @param cacheCapacity
     *            The capacity of the backing hard reference queue.
     * @param cacheTimeoutMillis
     *            The timeout (milliseconds) before an entry which has not been
     *            touched is cleared from the cache.
     */
    public IndexCache(final int cacheCapacity, final long cacheTimeoutMillis) {

        super(cacheCapacity, TimeUnit.MILLISECONDS.toNanos(cacheTimeoutMillis));

    }
    
    /**
     * The earliest timestamp that MUST be retained for the read-historical
     * indices in the cache and {@link Long#MAX_VALUE} if there are NO
     * read-historical indices in the cache.
     * <p>
     * Note: Due to the concurrent operations of the garbage collector, this
     * method MAY return a time that is earlier than necessary. This
     * possibility arises because {@link WeakReference} values may be
     * cleared at any time. There is no negative consequence to this
     * behavior - it simply means that fewer resources will be released than
     * might otherwise be possible. This "hole" can not be closed by polling
     * the {@link ReferenceQueue} since it is not when the entries are
     * removed from the map which matters, but when their
     * {@link WeakReference} values are cleared. However, periodically
     * clearing stale references using {@link #clearStaleRefs()} will keep
     * down the size of that "hole".
     */
    public long getRetentionTime() {

//            /*
//             * This will tend to clear stale references and cause them to be
//             * cleared in the cache, but probably not in time for the changes to
//             * be noticed within the loop below.
//             */
//
//            clearStaleRefs();
        
        synchronized (retentionTime) {

            if (retentionTime.get() == -1L) {

                // note: default if nothing is found.
                long tmp = Long.MAX_VALUE;

                int n = 0;

                final long now = System.currentTimeMillis();

                final Iterator<Map.Entry<NT, WeakReference<H>>> itr = entryIterator();

                while (itr.hasNext()) {

                    final Map.Entry<NT, WeakReference<H>> entry = itr.next();

                    if (entry.getValue().get() == null) {

                        // skip cleared references.
                        continue;

                    }

                    final long timestamp = entry.getKey().getTimestamp();

                    if (timestamp == ITx.UNISOLATED) {

                        // ignore unisolated indices.
                        continue;

                    }

                    if(debug)
                        log.warn("considering: " + entry.getKey());
                    
                    if (timestamp < tmp) {

                        // choose the earliest timestamp for any index.
                        tmp = timestamp;
                        
                        if (debug)
                            log.warn("Earliest so far: "
                                    + tmp
                                    + " (age="
                                    + TimeUnit.MILLISECONDS
                                            .toSeconds(tmp - now) + "secs)");

                    }
                    
                    n++;

                } // next entry

                retentionTime.set(tmp);

                if (debug)
                    log.warn("Considered: " + n + " indices: retentionTime="
                            + tmp + " (age="
                            + TimeUnit.MILLISECONDS.toSeconds(tmp - now)
                            + "secs)");
                
            } // end search.

            return retentionTime.get();
            
        } // synchronized(retentionTime)

    }

    @Override
    public H put(final NT k, final H v) {

        synchronized (retentionTime) {

            if (retentionTime.get() > k.getTimestamp()) {

                // found an earlier timestamp, so use that.
                retentionTime.set(k.getTimestamp());

            }

        }

        return super.put(k, v);

    }

    @Override
    public H putIfAbsent(final NT k, final H v) {
        
        synchronized(retentionTime) {
            
            if (retentionTime.get() > k.getTimestamp()) {

                // found an earlier timestamp, so use that.
                retentionTime.set(k.getTimestamp());
                
            }
            
        }

        return super.putIfAbsent(k, v);
        
    }
    
    /**
     * Overridden to clear the {@link #retentionTime} if the map entry
     * corresponding to that timestamp is being removed from the map.
     */
    @Override
    protected WeakReference<H> removeMapEntry(final NT k) {
        
        synchronized(retentionTime) {
            
            if (retentionTime.get() == k.getTimestamp()) {

                /*
                 * Removed the earliest timestamp so we will need to
                 * explicitly search for the new minimum timestamp.
                 */
                
                retentionTime.set(-1L);
                
            }
            
        }

        return super.removeMapEntry(k);
        
    }
    
}
