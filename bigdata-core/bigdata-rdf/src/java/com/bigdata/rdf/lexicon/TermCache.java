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
 * Created on Jan 2, 2012
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.cache.ConcurrentWeakValueCacheWithBatchedUpdates;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermCache<K extends IV<?, ?>, V extends BigdataValue> implements
        ITermCache<K, V> {

    private final ConcurrentWeakValueCacheWithBatchedUpdates<IV<?, ?>, V> delegate;

    public TermCache(
            final ConcurrentWeakValueCacheWithBatchedUpdates<IV<?, ?>, V> delegate) {

        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;

    }
    
    @Override
    public int size() {
        
        return delegate.size();
        
    }
    
    @Override
    public V get(final K k) {
        
        return delegate.get(k);

    }

    /**
     * {@inheritDoc}
     * <p>
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/437 (Thread-local
     *      cache combined with unbounded thread pools causes effective memory
     *      leak)
     */
    @Override
    public V putIfAbsent(final K k, final V v) {

        V tmp = delegate.get(k);
        
        if(tmp != null) {
        
            // No need to write on the map.
            return tmp; 
            
        }

        /*
         * Clone the IV in order to ensure that the hard reference from the IV
         * to the BigdataValue cached on the IV has been cleared before we enter
         * the IV into the map.
         * 
         * Note: If the key has a hard reference to the value then the value can
         * never become only weakly reachable. That turns the term cache into a
         * a memory leak! This is why we break the IV.cache reference here.
         * 
         * Note: We do not need to break the cache reference for the vocabulary
         * IVs. They are already pinned by a hard reference and hence will never
         * become only weakly reachable. There is also only a limited number of
         * such vocabulary IVs so the size of the map never becomes a problem.
         * Finally, the vocabulary IVs tend to be frequently used, so having
         * them pinned is useful.
         */

        return delegate.putIfAbsent(k.clone(true/*clearCache*/), v);

    }

    @Override
    public void clear() {
        
        delegate.clear();
        
    }
    
}
