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
 * Created on Mar 22, 2009
 */

package com.bigdata.counters.store;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.History;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.journal.CommitRecordIndex.Entry;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.sparse.SparseRowStore;

/**
 * An API encapsulating for writing and querying counter sets. The data are
 * written onto an {@link IIndex}. The {@link IIndex} may be local or remote.
 * <p>
 * The multipart key is used. The first component is the milliseconds of the
 * associated timestamp value rounded down to an even number of minutes and
 * represented a long. The second component is the fully qualified path of the
 * counter. The last component is the exact timestamp (in milliseconds) of the
 * sampled counter value, represented as a long. These are formatted into an
 * unsigned byte[] following the standard practice.
 * <p>
 * The value stored under the key is the counter value. Normally counter values
 * are doubles or longs, but you can store any of the counter value types which
 * are supported by the {@link SparseRowStore}.
 * <p>
 * Using this approach, writes of the same counter value with different
 * timestamps will be recorded as different tuples in the {@link IIndex} and you
 * can store counter values sampled at rates of once per second while retaining
 * good compression for the keys in the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetBTree extends BTree {

    protected static transient final Logger log = Logger
            .getLogger(CounterSetBTree.class);
    
    /**
     * @param store
     * @param checkpoint
     * @param metadata
     */
    public CounterSetBTree(IRawStore store, Checkpoint checkpoint,
            IndexMetadata metadata) {

        super(store, checkpoint, metadata);

    }

    static private final transient int INITIAL_CAPACITY = Bytes.kilobyte32;

    /**
     * Create a new instance.
     * 
     * @param store
     *            The backing store.
     * 
     * @return The new instance.
     */
    static public CounterSetBTree create(final IRawStore store) {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBTreeClassName(CounterSetBTree.class.getName());

        metadata.setTupleSerializer(new CounterSetBTreeTupleSerializer(
                new ASCIIKeyBuilderFactory(INITIAL_CAPACITY)));

        return (CounterSetBTree) BTree.create(store, metadata);

    }

    static public CounterSetBTree createTransient() {

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBTreeClassName(CounterSetBTree.class.getName());

        metadata.setTupleSerializer(new CounterSetBTreeTupleSerializer(
                new ASCIIKeyBuilderFactory(INITIAL_CAPACITY)));

        return (CounterSetBTree) BTree.createTransient(metadata);

    }

    /**
     * Encapsulates key and value formation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class CounterSetBTreeTupleSerializer extends
            DefaultTupleSerializer<Long, Entry> {

        /**
         * De-serialization ctor.
         */
        public CounterSetBTreeTupleSerializer() {

            super();

        }

        /**
         * Ctor when creating a new instance.
         * 
         * @param keyBuilderFactory
         */
        public CounterSetBTreeTupleSerializer(
                final IKeyBuilderFactory keyBuilderFactory) {

            super(keyBuilderFactory);

        }

        /**
         * Return the unsigned byte[] key for an {@link ICounter}.
         * 
         * @param obj
         *            An {@link ICounter}.
         */
        @Override
        public byte[] serializeKey(Object obj) {

            final ICounter c = (ICounter) obj;

            final long timestamp = c.lastModified();

            return getKeyBuilder().reset()//
                    .append(TimeUnit.MILLISECONDS.toMinutes(timestamp))//
                    .appendASCII(c.getPath())//
                    .append(timestamp)//
                    .getKey();

        }

    }

    /**
     * Writes the current value of each visited {@link ICounter} on the store.
     * <p>
     * Note: This presumes that the counters are associated with scalar values
     * (rather than {@link History}s).
     * 
     * @todo More efficient storage for Double, Long, Integer and String values
     *       (this is using Java default serialization)?
     * 
     * @todo Efficient for batch operations? Note that the counter set iterator
     *       will normally be in alpha order already and all samples should be
     *       close to the same minute, so this is already efficient for local
     *       operations.
     */
    public void insert(final Iterator<ICounter> src) {

        while (src.hasNext()) {

            final ICounter c = src.next();

            if(log.isDebugEnabled()) {
                
                log.debug(c);
                
            }
            
            insert(c, c.getValue());

        }

    }

    /**
     * 
     * @param fromTime
     *            The first time whose counters will be visited.
     * @param toTime
     *            The first time whose counters WILL NOT be visited.
     * @param unit
     *            The unit in which <i>fromTime</i> and <i>toTime</i> are
     *            expressed.
     * @param filter
     *            Only paths matched by the filter will be accepted (optional).
     * @param granularity
     * @return
     * 
     * @todo In an act of cowardice, this assumes that the counter paths are
     *       ASCII and encodes them as such. This allows us to decode the
     *       counter path since it is not a compressed sort key. If we don't
     *       take this "hack" then we need a 2nd index to resolve the Unicode
     *       path from the sort key (once we hack off the leading minutes
     *       component).
     *       <p>
     *       The other problem is that tacking the milliseconds onto the end of
     *       the key might break the natural order of the counter paths in the
     *       index.
     *       <p>
     *       The two index approach is not so bad. The main drawback is that it
     *       can't be encapsulated as easily.
     */
    public CounterSet rangeIterator(long fromTime, long toTime, TimeUnit unit,
            Pattern filter, TimeUnit granularity,
            IInstrumentFactory instrumentFactory) {

        final IKeyBuilder keyBuilder = getIndexMetadata().getTupleSerializer()
                .getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().append(
                unit.toMinutes(fromTime)).getKey();

        final byte[] toKey = keyBuilder.reset().append(unit.toMinutes(toTime))
                .getKey();

        final CounterSet counters = new CounterSet();

        final ITupleIterator itr = rangeIterator(fromKey, toKey);

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();
            
            final byte[] key = tuple.getKey();
            
//            final long minutes = KeyBuilder.decodeLong(key, 0/* off */);

            final String path = KeyBuilder
                    .decodeASCII(key, Bytes.SIZEOF_LONG/* off */, key.length
                            - (2*Bytes.SIZEOF_LONG)/* len */);

            if (filter != null && !filter.matcher(path).matches()) {

                if (log.isDebugEnabled()) {

                    log.debug("Rejected: " + path);
                    
                }
                
                continue;
                
            }
            
            final long timestamp = KeyBuilder.decodeLong(key, key.length
                    - Bytes.SIZEOF_LONG/*off*/);

            final Object value = tuple.getObject();
            
            ICounterNode c = counters.getPath(path);
            final IInstrument inst;
            
            if( c == null ) {
                
                /*
                 * FIXME This does not allow us to choose the granularity of the
                 * history and does not allow us to inform the history of the
                 * #of samples which it must be able to record. Track down all
                 * use of the HistoryInstrument ctor and modify it to have an
                 * explicit array dimension as well as the array type, at which
                 * point we might as well just pass in the buffer.
                 */
                inst = instrumentFactory.newInstance(value.getClass());
                
                c = counters.addCounter(path, inst);
                
            } else if (c instanceof ICounterSet) {

                log.error("CounterSet exists for counter path: " + path);
                
                continue;
                
            } else {
                
                inst = ((ICounter) c).getInstrument();

            }

            inst.setValue(value, timestamp);
            
        }

        return counters;
        
    }

}
