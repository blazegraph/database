/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jul 30, 2010
 */

package com.bigdata.relation.rule.eval;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.pipeline.JoinTaskFactoryTask;

/**
 * A class which implements an in memory distributed hash table (DHT) which can
 * be used to filter for distinct elements. A factory is provided for
 * instantiating the DHT.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <K>
 * 
 * @todo The outer class is similar in nature to {@link JoinTaskFactoryTask}
 * 
 * @todo could have an implementation backed by a persistent hash map using an
 *       extensible hash function to automatically grow the persistence store
 *       (this could be a general purpose persistent hash functionality).
 */
public class DHTFilterFactory<K> {

    /**
     * The UUID associated with the query.
     */
    private final UUID masterUUID;
    
    public DHTFilterFactory(final UUID masterUUID) {

        this.masterUUID = masterUUID;
        
    }

    /**
     * A concurrent hash table living on some node and tied to some query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @param <K>
     * @param <V>
     * 
     * @todo Consider the use of lock amortization (batching) to reduce
     *       contention for the backing map. Alternatively, we could accept
     *       entire blocks of elements from a single source at a time, which
     *       would single thread us through the map. Or bound the #of threads
     *       hitting the map at once, increase the map concurrency level, etc.
     */
    public static class DHTFilter<K> implements IElementFilter<K> {

        private final UUID masterUUID;

        private final ConcurrentHashMap<K, Void> map;

        public DHTFilter(final UUID masterUUID) {
            
            this.masterUUID = masterUUID;
            
            this.map = new ConcurrentHashMap<K, Void>(/*
                                                       * initialCapacity,
                                                       * loadFactor,
                                                       * concurrencyLevel
                                                       */);

        }

        public boolean accept(final K e) {
         
            return map.putIfAbsent(e, null) == null;
            
        }

        public boolean canAccept(Object o) {

            return true;
            
        }

//        public ResultBitBuffer bulkFilter(final K[] elements) {
//            
//        }
        
    }
    
}
