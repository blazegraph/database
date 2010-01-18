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

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.History;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.History.SampleIterator;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.io.SerializerUtil;
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
 * 
 * FIXME Reading through per-minute counters from a CounterSetBTree grows slow
 * very quickly.
 * 
 * <pre>
 * There are 21750988 counter values covering Fri Apr 03 15:51:57 EDT 2009 to
 * Sat Apr 04 08:45:05 EDT 2009. Took 60 seconds to record each hour of data on
 * the disk.  1.2G of XML data expanded to 2.6G on the journal
 * </pre>
 * 
 * In order to improve performance, put the counter paths in a separate
 * dictionary and apply the regex there. Once we have the set of matched paths
 * we can scatter range queries against the BTree and drag back the data for
 * those counters (this would also make Unicode counter names viable). If the
 * key was then [pathId,timestamp] we could do ordered reads of just the
 * necessary key range for each desired counter. Prefix compression would still
 * be efficent for this representation. While the data arrive in history blocks,
 * we would still need to buffer them for ordered writes since otherwise the
 * writes would be scattered by the first key component (pathId).
 * <p>
 * I would have to encapsulate the counters as a counter for this to work, much
 * like the RDF DB. There would be two relations: the dictionary and the
 * timestamped values.
 * <p>
 * Space efficient encoding of the counter values would also help quite a bit -
 * it is Java default serialization, but we only store Long, Double or String.
 * All values for a given counter should have the same data type (it is required
 * by how we allocate the History) so the data type can be part of the
 * dictionary and that can be used to decode the value. (If values tend to be
 * close then a delta encoding would help.)
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
            IndexMetadata metadata, boolean readOnly) {

        super(store, checkpoint, metadata, readOnly);

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
     * A representation of a timestamped performance counter value as stored in
     * the {@link CounterSetBTree}. The minutes, path, and timestamp fields are
     * recovered from the key. The counter value is recovered from the value.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class Entry {

        // key
        public final String path;
        public final long timestamp;
        
        // value
        public final Object value;
       
        public Entry(final long timestamp,
                final String path, final Object value) {

            this.timestamp = timestamp;
            this.path = path;
            this.value = value;
            
        }
        
        public String toString() {
            
            return getClass().getName()+//
            "{ path="+path+//
            ", value="+value+//
            ", timestamp="+timestamp+//
            "}";
            
        }

        /**
         * Return the depth of the path in the performance counter hierarchy
         * (counts the #of '/' characters in the path).
         * 
         * @return The depth.
         */
        public int getDepth() {
            int depth = 0;
            final int len = path.length();
            for (int i = 0; i < len; i++) {
                if (path.charAt(i) == '/') {
                    depth++;
                }
            }
            return depth;
        }
        
    }
    
    /**
     * Encapsulates key and value formation. The key is formed from the minutes,
     * the path, and the timestamp. The value is the performance counter value
     * for a specific timestamp.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class CounterSetBTreeTupleSerializer extends
            DefaultTupleSerializer<Object, Entry> {

        /**
         * 
         */
        private static final long serialVersionUID = -887369151228567134L;

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
         * Return the unsigned byte[] key.
         * 
         * @param obj
         *            An {@link ICounter} or {@link Entry}.
         */
        @Override
        public byte[] serializeKey(final Object obj) {

            if (obj == null)
                throw new IllegalArgumentException();
            
            if(obj instanceof ICounter) {
                
                return serializeKey((ICounter)obj);
                
            } else if(obj instanceof Entry) {
                
                return serializeKey((Entry)obj);
                
            } else {
                
                throw new UnsupportedOperationException(obj.getClass().getName());
                
            }
            
        }

        public byte[] serializeKey(final ICounter c) {

            final long timestamp = c.lastModified();

            return getKeyBuilder().reset()//
                    .append(TimeUnit.MILLISECONDS.toMinutes(timestamp))//
                    .appendASCII(c.getPath())//
                    .append(timestamp)//
                    .getKey();

        }
        
        public byte[] serializeKey(final Entry e) {

            return getKeyBuilder().reset()//
                    .append(TimeUnit.MILLISECONDS.toMinutes(e.timestamp))//
                    .appendASCII(e.path)//
                    .append(e.timestamp)//
                    .getKey();
            
        }

        public Entry deserialize(final ITuple tuple) {

            final byte[] key = tuple.getKey();

            // final long minutes = KeyBuilder.decodeLong(key, 0/* off */);

            final String path = KeyBuilder.decodeASCII(key,
                    Bytes.SIZEOF_LONG/* off */, key.length
                            - (2 * Bytes.SIZEOF_LONG)/* len */);

            final long timestamp = KeyBuilder.decodeLong(key, key.length
                    - Bytes.SIZEOF_LONG/* off */);

            // @todo tuple.getValueStream()
            final Object value = SerializerUtil.deserialize(tuple.getValue());

            return new Entry(timestamp, path, value);
            
        }

    }

    /**
     * Handles efficient writes of counters with {@link History} data. The shape
     * of the data is changed so that the resulting writes on the BTree will be
     * ordered. This is both faster and also results in a smaller size on the
     * size (since leaves are not updated once they are written to the store).
     * For a counter without history, the current value of the counter will be
     * written on the BTree.
     */
    public void writeHistory(final Iterator<ICounter> src) {
        
        final long begin = System.currentTimeMillis();
        
        final Vector<KVO<Entry>> v = new Vector<KVO<Entry>>();
        
        final CounterSetBTreeTupleSerializer tupleSer = (CounterSetBTreeTupleSerializer) getIndexMetadata()
                .getTupleSerializer();
        
        while (src.hasNext()) {

            final ICounter c = src.next();

            final String path = c.getPath();

            if (c.getInstrument() instanceof HistoryInstrument) {

                final History h = ((HistoryInstrument) (c.getInstrument()))
                        .getHistory();

                final SampleIterator sitr = h.iterator();

                while (sitr.hasNext()) {

                    final IHistoryEntry e = sitr.next();

                    final Entry entry = new Entry(e.lastModified(), path, e
                            .getValue());

                    final byte[] key = tupleSer.serializeKey(entry);

                    final byte[] val = tupleSer.serializeVal(entry.value);

                    v.add(new KVO<Entry>(key, val, entry));

                }
                
            } else {
            
                final Entry entry = new Entry(c.lastModified(), path, c
                        .getValue());
                
                final byte[] key = tupleSer.serializeKey(entry);
                
                final byte[] val = tupleSer.serializeVal(entry.value);
                
                v.add(new KVO<Entry>(key, val, entry));
                
            }
            
        }

        // to array
        final KVO[] a = v.toArray(new KVO[v.size()]);
        
        // order by the key.
        Arrays.sort(a);

        long nwritten = 0;
        
        // ordered write on the BTree.
        for (KVO t : a) {

            /*
             * Note: Don't overwrite if we already have the timestamped counter
             * value in the store.
             */
            if (!super.contains(t.key)) {

                super.insert(t.key, t.val);

                nwritten++;
                
            }
            
        }

        final long elapsed = System.currentTimeMillis()-begin;
        
        if(log.isInfoEnabled()) {
            
            log.info("Wrote " + nwritten + " of " + a.length + " tuples in "
                    + elapsed + "ms");
            
        }
        
    }
    
    /**
     * Writes the <strong>current</strong> value of each visited
     * {@link ICounter} on the store.
     * <p>
     * Note: This presumes that the counters are associated with scalar values
     * (rather than {@link History}s).
     * <p>
     * Note that the counter set iterator will normally be in alpha order
     * already and all samples should be close to the same minute, so this is
     * already efficient for local operations.
     * 
     * @todo More efficient storage for Double, Long, Integer and String values
     *       (this is using Java default serialization)?
     */
    public void writeCurrent(final Iterator<ICounter> src) {

        while (src.hasNext()) {

            final ICounter c = src.next();

            if(log.isDebugEnabled()) {
                
                log.debug(c);
                
            }
         
//            if (c.getInstrument() instanceof HistoryInstrument) {
//
//                /*
//                 * This handles a history counter. However, loading a set
//                 * history counters will cause writes to be scattered across the
//                 * index since the counters are processed in alpha (path) order
//                 * but each counter has a history (ascending minutes). In order
//                 * to be efficient, the histories need to be converted into a
//                 * KVO[] and then sorted before doing a bulk insert.
//                 */
//                
//                final String path = c.getPath();
//                
//                final History h = ((HistoryInstrument)c.getInstrument()).getHistory();
//                
//                final SampleIterator sitr = h.iterator();
//                
//                while(sitr.hasNext()) {
//                    
//                    final IHistoryEntry hentry = sitr.next();
//                    
//                    // entry reporting the average value for the history slot.
//                    final Entry entry = new Entry(hentry.lastModified(), path,
//                            hentry.getValue());
//                    
//                    insert(entry, entry.value);
//                    
//                }
//                
//            } else {
                
                // just the current value of the counter.
                insert(c, c.getValue());
                
//            }

        }

    }

    /**
     * <strong>The toTime needs to be ONE (1) <i>unit</i> beyond the time of
     * interest since the minutes come first in the key. If you do not follow
     * this rule then you can miss out on the last <i>unit</i> worth of data.</strong>
     * 
     * @param fromTime
     *            The first time whose counters will be visited (in
     *            milliseconds).
     * @param toTime
     *            The first time whose counters WILL NOT be visited (in
     *            milliseconds).
     * @param unit
     *            The unit of aggregation for the reported counters.
     * @param filter
     *            Only paths matched by the filter will be accepted (optional).
     * @param depth
     *            When non-zero, only counters whose depth is LTE to the
     *            specified <i>depth</i> will be returned.
     * 
     * @return A collection of the selected performance counters together with
     *         their ordered timestamped values for the specified time period.
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
    public CounterSet rangeIterator(long fromTime, long toTime,
            final TimeUnit unit, final Pattern filter, final int depth) {

        if (fromTime < 0)
            throw new IllegalArgumentException();

        if (toTime < 0)
            throw new IllegalArgumentException();
        
        if (unit == null)
            throw new IllegalArgumentException();
        
        if (fromTime == 0L) {
            /*
             * Default is the first available timestamp.
             */
            fromTime = getFirstTimestamp();
        }

        if (toTime == 0L || toTime == Long.MAX_VALUE) {
            /*
             * Default is the last available timestamp.
             */
            toTime = getLastTimestamp();
        }
        
        /*
         * Convert the covered time span into the caller's unit of aggregation.
         * 
         * Note: The +1 is required to allocate enough slots in the History.
         * Without it the History class can overwrite the first slot, which will
         * cause the data to be underreported for the first time period.
         */
        final long nslots = unit.convert(toTime, TimeUnit.MILLISECONDS)
                - unit.convert(fromTime, TimeUnit.MILLISECONDS) + 1;

        if (nslots > Integer.MAX_VALUE)
            throw new IllegalArgumentException("too many samples");

        final CounterSetBTreeTupleSerializer tupleSer = (CounterSetBTreeTupleSerializer) getIndexMetadata()
                .getTupleSerializer();

        final IKeyBuilder keyBuilder = getIndexMetadata().getTupleSerializer()
                .getKeyBuilder();

        /*
         * Note: The first field in the key is the counter timestamp converted
         * to minutes since the epoch. Therefore we need to take the fromTime
         * milliseconds and convert it to minutes. Since that conversion
         * truncates the value, we will always have a fromKey that is EQ to the
         * minute in which the counters with a [fromTime] timestamp would be
         * found.
         */
        final long fromMinutes = TimeUnit.MILLISECONDS.toMinutes(fromTime);
        final byte[] fromKey = keyBuilder.reset().append(fromMinutes).getKey();

        /*
         * Note: The [toKey] needs to be strictly GT the minute in which the
         * [toTime] would be found. This may overscan, but that is better than
         * failing to scan enough. Any overscan is filtered out below.
         */
        final long toMinutes = TimeUnit.MILLISECONDS.toMinutes(toTime
                + TimeUnit.MINUTES.toMillis(1));
        final byte[] toKey = keyBuilder.reset().append(toMinutes).getKey();

        if(log.isInfoEnabled()) {
            
            log.info("fromTime=" + fromTime + "ms (" + fromMinutes
                    + "m), toTime=" + toTime + "ms (" + toMinutes
                    + "m), units=" + unit + ", nslots=" + nslots);

        }

        // iterator scanning the counters.
        final ITupleIterator itr = rangeIterator(fromKey, toKey);

        // #of distinct counter paths selected by the query.
        int nselected = 0;
        
        // #of timestamp counter values accepted.
        long nvalues = 0;
        
        // #of tuples (aka timestamped counter values) visited.
        long nvisited = 0;

        // counters are inserted into this collection.
        final CounterSet counters = new CounterSet();

        // factory for history counters.
        final IInstrumentFactory instrumentFactory = new DefaultInstrumentFactory(
                (int) nslots, PeriodEnum.getValue(unit), false/* overwrite */);

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();
            
            nvisited++;
            
            final Entry entry = tupleSer.deserialize(tuple);

            if (fromTime < entry.timestamp || toTime >= entry.timestamp) {

                /*
                 * Due to the leading [minutes] field in the key there can be
                 * some underscan and overscan of the index. Therefore we filter
                 * to ensure that only timestamps which are strictly within the
                 * specified milliseconds are extracted.
                 */
                
                if (log.isTraceEnabled()) {

                    log.trace("Rejected: minutes="
                            + TimeUnit.MILLISECONDS.toMinutes(entry.timestamp)
                            + " : " + entry.path);
                    
                }

            }

            if (depth != 0 && depth > entry.getDepth()) {

                if (log.isTraceEnabled()) {

                    log.trace("Rejected: minutes="
                            + TimeUnit.MILLISECONDS.toMinutes(entry.timestamp)
                            + " : " + entry.path);
                    
                }

            }
            
            if (filter != null && !filter.matcher(entry.path).matches()) {

                if (log.isTraceEnabled()) {

                    log.trace("Rejected: minutes="
                            + TimeUnit.MILLISECONDS.toMinutes(entry.timestamp)
                            + " : " + entry.path);
                    
                }
                
                continue;
                
            }

            ICounterNode c = counters.getPath(entry.path);
            final IInstrument inst;

            if (c == null) {

                // log first time matched for each path.
                if (log.isDebugEnabled()) {

                    log.debug("Matched: ndistinct=" + nselected + ", "
                            + entry.path);
                    
                }

                nselected++;

                inst = instrumentFactory.newInstance(entry.value.getClass());

                c = counters.addCounter(entry.path, inst);

            } else if (c instanceof ICounterSet) {

                log.error("CounterSet exists for counter path: " + entry.path);

                continue;

            } else {

                inst = ((ICounter) c).getInstrument();

            }

            inst.setValue(entry.value, entry.timestamp);

            nvalues++;
            
        }

        if (log.isInfoEnabled())
            log.info("nselected=" + nselected + ", nvalues=" + nvalues
                    + ", nvisited=" + nvisited);

        return counters;
        
    }

    /**
     * Return the timestamp associated with the first performance counter value.
     * 
     * @return The timestamp -or- 0L if there are no performance counter values.
     */
    public long getFirstTimestamp() {

        if (getEntryCount() == 0)
            return 0L;

        return ((Entry) rangeIterator(null, null, 1/* capacity */,
                IRangeQuery.DEFAULT, null/* filter */).next().getObject()).timestamp;

    }
    
    /**
     * Return the timestamp associated with the last performance counter value.
     * 
     * @return The timestamp -or- 0L if there are no performance counter values.
     */
    public long getLastTimestamp() {

        if (getEntryCount() == 0)
            return 0L;

        return ((Entry) rangeIterator(null, null, 1/* capacity */,
                IRangeQuery.DEFAULT | IRangeQuery.REVERSE, null/* filter */)
                .next().getObject()).timestamp;

    }
    
}
