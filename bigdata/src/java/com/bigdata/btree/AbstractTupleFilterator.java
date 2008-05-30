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
 * Created on May 30, 2008
 */

package com.bigdata.btree;

import it.unimi.dsi.fastutil.Stack;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.btree.IDataSerializer.SimplePrefixSerializer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.sparse.AtomicRead;
import com.bigdata.sparse.AtomicRowScan;
import com.bigdata.sparse.INameFilter;
import com.bigdata.sparse.KeyDecoder;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.sparse.TPS;
import com.bigdata.sparse.ValueType;

/**
 * Abstract base class for filter or transforming {@link ITupleIterator}s. This
 * class provides a general purpose mechanism that may be used to layer the
 * semantics of an {@link ITupleIterator}. The primary use cases are filtering
 * of tuples based on their key and/or value and aggregation of atomic reads to
 * higher level data structures such as the {@link AtomicRowScan}.
 * 
 * FIXME he {@link AbstractTupleFilterator} is now a full {@link ITupleIterator}
 * and it WRAPS the base iterator requested by the caller. It can be used
 * directly when you know that you are operating on a local index or you can use
 * the {@link AbstractTupleFilteratorConstructor} when you need to describe the
 * filter on the client and have the wrapper setup for you on the server.
 * 
 * FIXME Modify {@link IRangeQuery} to change the type of the "filter" argument
 * to a factory object for an {@link AbstractTupleFilterator}, e.g.,
 * {@link AbstractTupleFilteratorConstructor} so that we can describe the filter
 * on the client and have it instantiated with its state on the server.
 * 
 * FIXME Modify {@link TupleIterator} to use
 * {@link Node#getLeftSibling(AbstractNode, boolean)} and
 * {@link Node#getRightSibling(AbstractNode, boolean)} and to traverse the leafs
 * in either order and without the post-order striterator (but we still need
 * post- order traversal for flushing evicted nodes to the store!) We need to
 * keep the hard reference stack of the parent nodes during the traversal
 * because the child NEVER has the address of its parent (this is true for the
 * index segment also).
 * 
 * FIXME Support {@link TupleIterator#remove()}. The iterator needs to touch up
 * its state when a tuple is deleted. This can not be treated directly within
 * remove() since deleting a tuple can cause a leaf to underflow resulting in a
 * cascade of structural changes to the B+Tree. We need a listener that is
 * registered by the iterator and notices when a structural change has
 * invalidated either the leaf on which it is sitting or the location of the
 * tuple (it may have been rotated to another leaf or even deleted). It then
 * needs to re-establish the stack of hard references from the root (which may
 * have changed) all the way down to whatever leaf has the insertion point for
 * the tuple that was just deleted. next() or prior() will then do the right
 * thing from there. This implies that the TupleIterator needs a {@link Node}
 * {@link Stack} and must not use recursion when it establishes the point from
 * which it needs to traverse - either when it delivers the first tuple or after
 * the structure has been invalidated. This will also let the iterator survive
 * concurrent modification of the BTree by code in the same thread (it is not
 * thread-safe for writes, but you can write on the index while running through
 * the iterator and it will do the right thing). This is similar to how GOM
 * handles traversal of the linked list.
 * 
 * FIXME Get rid of the nasty code using {@link ChunkedLocalRangeIterator} to
 * handle {@link IRangeQuery#REMOVEALL}
 * 
 * FIXME There are some bugs in the bigdata services package that appear to all
 * be linked to a problem with the {@link ChunkedLocalRangeIterator}. If I can
 * just get rid of that then I will be much happier!
 * 
 * FIXME Add a variant to {@link IRangeQuery} that accepts an array of keys and
 * maps a caller supplied {@link AbstractTupleFilterator} across those keys.
 * This will be used for the prefix scan.
 * 
 * FIXME The distinct term scan can be written just yet. It needs to advance the
 * {@link ITupleIterator} to a key formed from the successor of the current key.
 * In order to support that {@link TupleIterator} needs to support the "gotoKey"
 * (as long as it is within the half-open range) and an
 * {@link AbstractTupleFilterator} can then be written that advances the source
 * iterator. Since it will all be "just an iterator" the client code will map it
 * across the index partitions without change. (Note: this does suggest that
 * skipping backwards could cause the client code to not terminate if the
 * filterator has its logic wrong).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTupleFilterator implements ITupleIterator {

    protected static final Logger log = Logger.getLogger(AbstractTupleFilterator.class);
    
    protected final ITupleIterator src;
    
    private ITuple current = null;
    
    protected AbstractTupleFilterator(ITupleIterator src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public ITuple next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        assert current != null;

        final ITuple t = current;

        current = null;

        return t;

    }

    public boolean hasNext() {

        if (current != null)
            return true;

        /*
         * Scan ahead until we find the next tuple that satisifies the filter.
         */
        while (src.hasNext()) {

            final ITuple tuple = src.next();

            if (isValid(tuple)) {

                current = tuple;

                return true;

            }

        }

        return false;

    }

    /**
     * @todo not implemented because we are using a lookahead(1) paradigm and
     *       the source iterator will be on a different tuple than the last one
     *       returned by {@link #next()} if {@link #hasNext()} was been invoked
     *       before invoking {@link #remove()} -- test this out to verify.  how
     *       does the striterator handle this?
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    /**
     * Return true iff the tuple should be visited.
     * 
     * @param tuple
     *            The tuple.
     */
    abstract protected boolean isValid(ITuple tuple);
    
    /**
     * Class creates configured instances of an {@link AbstractTupleFilterator} so
     * that the filter may be executed remotely.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractTupleFilteratorConstructor implements /*ITupleFilter*/ Externalizable{
        
        public void readExternal(ObjectInput arg0) throws IOException, ClassNotFoundException {

            // NOP
            
        }

        public void writeExternal(ObjectOutput arg0) throws IOException {
            
            // NOP
            
        }

    }
    
    /**
     * Abstract base class for an {@link ITupleIterator} that transforms the
     * data type of the keys and/or values.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME make sure that the various compression and serialization handlers
     * specified here are imposed by the {@link ResultSet} which MUST notice
     * this class and use its properties in preference to those available from
     * the {@link IndexMetadata}
     */
    abstract public static class AbstractTransformingTupleFilter extends AbstractTupleFilterator {

        final protected int flags;
        final protected IDataSerializer leafKeySer;
        final protected IDataSerializer leafValSer;
        final protected ITupleSerializer tupleSer;
        
        /**
         * 
         * @param src
         *            The source iterator.
         * @param flags
         *            The flags specified for the source iterator.
         * @param leafKeySer
         *            The compression provider for the key[].
         * @param leafValSer
         *            The compression provider for the value[].
         * @param tupleSer
         *            The serialization provider for the individual tuples.
         */
        protected AbstractTransformingTupleFilter(ITupleIterator src,
                int flags, IDataSerializer leafKeySer,
                IDataSerializer leafValSer, ITupleSerializer tupleSer) {

            super(src);

            if (leafKeySer == null)
                throw new IllegalArgumentException();

            if (leafValSer == null)
                throw new IllegalArgumentException();

            if (tupleSer == null)
                throw new IllegalArgumentException();

            this.flags = flags;
            
            this.leafKeySer = leafKeySer;

            this.leafValSer = leafValSer;
            
            this.tupleSer = tupleSer;

        }
        
    }
    
//    /**
//     * Logical row scan for the {@link SparseRowStore}.
//     * 
//     * FIXME write this two ways.
//     * 
//     * First as a look ahead filter that enforces logical row boundaries and
//     * filters out tuples whose column name or timestamp do not satisify the
//     * query.
//     * 
//     * Second as a {@link AbstractTransformingTupleFilter} that sends back a
//     * logical row in each tuple.
//     * 
//     * Note that neither version can be {@link Serializable} since the need
//     * access to the source iterator so we need either a setSource(...) method
//     * or a constructor object for the iterator that sends along the state and
//     * create the iterator on the server.
//     * 
//     * FIXME figure out which implementation is "better" and move this to the
//     * correct package.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class AtomicRowIterator1 extends AbstractTupleFilter {
//        
//        protected static final Logger log = Logger.getLogger(AtomicRowIterator1.class);
//
//        final private long timestamp;
//        final private INameFilter nameFilter;
//        
//        /**
//         * @param src
//         */
//        protected AtomicRowIterator1(ITupleIterator src, long timestamp,
//                INameFilter nameFilter) {
//
//            super(src);
//            
//            this.timestamp = timestamp;
//            
//            this.nameFilter = nameFilter;
//            
//        }
//
//        /**
//         * Reads zero or more logical rows, applying the timestamp and the optional
//         * filter.
//         * 
//         * @return A serialized representation of those rows together with either
//         *         the <i>fromKey</i> from which the scan should continue or
//         *         <code>null</code> iff the procedure has scanned all logical
//         *         rows in the specified key range (i.e., it has observed a primary
//         *         key that is GTE <i>toKey</i>). Note that the other termination
//         *         condition is when there are no more index partitions spanned by
//         *         the key range, which is handled by the {@link ClientIndexView}.
//         */
//        public TPSList apply(IIndex ndx) {
//         
//            while (hasNext()) {
//
//                // read the logical row from the index.
//                final TPS tps = AtomicRead.atomicRead(ndx, fromKey, schema,
//                        timestamp, nameFilter);
//
//                /*
//                 * Note: tps SHOULD NOT be null since null is reserved to indicate
//                 * that there was NO data for a given primary key (not just no
//                 * property values matching the optional constraints) and we are
//                 * scanning with a [fromKey] that is known to exist.
//                 */
//                assert tps != null;
//
//                // add to the result set.
//                rows.add(tps);
//                
//                /*
//                 * This is the next possible key with which the following logical
//                 * row could start. If we restart this loop we will scan the index
//                 * starting at this key until we find the actual key for the next
//                 * logical row. Otherwise this is where the scan should pick up if
//                 * we have reached the capacity on the current request.
//                 */
//                fromKey = SuccessorUtil.successor(fromKey);
//                
//                if (rows.size() >= capacity) {
//
//                    /*
//                     * Stop since we have reached the capacity for this request.
//                     */
//                    
//                    break;
//                    
//                }
//                
//            }
//
//            if (log.isInfoEnabled()) {
//
//                log.info("Read " + rows.size()+" rows: capacity="+capacity);
//            
//            }
//
//            /*
//             * Return both the key for continuing the scan and the ordered set of
//             * logical rows identified by this request.
//             */
//
//            return new TPSList(fromKey, rows.toArray(new TPS[rows.size()]));
//            
//        }
//
////        /**
////         * Scans the source iterator and reports the first key GTE to that key
////         * yet LE {@link #getToKey()}.
////         * <p>
////         * Note: This has a side-effect on the source iterator which is advanced
////         * to the first tuple in the next logical row.
////         * 
////         * @return The first key GTE the given key -or- <code>null</code> iff
////         *         there are NO keys remaining in the index partition GTE the
////         *         given key.
////         */
////        protected boolean advanceToNextLogicalRow() {
////            
////            while (src.hasNext()) {
////
////                final byte[] key = src.next().getKey();
////                
////                final KeyDecoder decoded = new KeyDecoder(key);
////
////                // extract just the schema and the primary key.
////                return decoded.getPrefix();
////
////            }
////
////            return null;
////            
////        }
//
//        /**
//         * Each visited tuple corresponds to a logical row. The key for the
//         * tuple is the {schema,primaryKey} for that logical row. The value is
//         * the serialized {@link TPS} object for the corresponding logical row.
//         * 
//         * @see ITuple#getObject()
//         */
//        public ITuple<ITPS> next() {
//            // TODO Auto-generated method stub
//            return null;
//        }
//
//        public boolean hasNext() {
//            // TODO Auto-generated method stub
//            return false;
//        }
//
//        public void remove() {
//            // TODO Auto-generated method stub
//            
//        }
//
//        /**
//         * Always returns <code>true</code>.
//         * 
//         * @todo could just pass an {@link ITupleIterator} instead of an
//         *       {@link ITupleFilter} for the server-side iterator override.
//         */
//        @Override
//        protected boolean isValid(ITuple tuple) {
//            
//            return true; // FIXME apply the name and timestamp filter here.
//            
//        }
//
//    }
    
    /**
     * Transforms an {@link ITupleIterator} reading directly on an
     * {@link IIndex} backing a {@link SparseRowStore} into an
     * {@link ITupleIterator} visiting logical rows.
     * 
     * @todo You could replace the {@link AtomicRead} with this iterator by
     *       setting the capacity to ONE (1). However, that will do more work
     *       when we are only trying to read a single row on a local index since
     *       we will have to serialize and then de-serialize the {@link TPS} for
     *       that logical row. (For a remote read the effort should be the
     *       same).
     * 
     * FIXME refactor so that I can apply this transforming iterator using the
     * standard
     * {@link IRangeQuery#rangeIterator(byte[], byte[], int, int, ITupleFilter)}.
     * When the index is local the "transforming filter" needs to wrap the
     * source iterator. When the index is remote, it still needs to wrap the
     * source iterator. So by saying "filter" we really mean something that
     * wraps and filters and/or transforms the source iterator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicRowIterator2 extends AbstractTransformingTupleFilter {
        
        private final int capacity;
        private final Schema schema;
        private final long timestamp;
        private final INameFilter nameFilter;
        
        /**
         * @param src
         *            The source iterator.
         * @param capacity
         *            The maximum #of logical rows that the iterator will read.
         * @param flags
         *            The flags specified for the source iterator.
         * @param schema
         *            The schema governing the row.
         * @param timestamp
         *            A timestamp to obtain the value for the named property
         *            whose timestamp does not exceed <i>timestamp</i> -or-
         *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most
         *            recent value for the property.
         * @param filter
         *            An optional filter used to select the values for property
         *            names accepted by that filter.
         */
        protected AtomicRowIterator2(ITupleIterator src, int capacity, int flags,
                Schema schema, long timestamp, INameFilter nameFilter) {

            super(src, flags, SimplePrefixSerializer.INSTANCE,
                    DefaultDataSerializer.INSTANCE, TPSTupleSerializer.INSTANCE);

            if (capacity <= 0)
                throw new IllegalArgumentException();
            
            if (schema == null)
                throw new IllegalArgumentException();
            
            this.capacity = capacity;
            
            this.schema = schema;

            this.timestamp = timestamp;

            this.nameFilter = nameFilter;
            
        }

        protected static final Logger log = Logger.getLogger(AtomicRowIterator2.class);
        
        /** #of logical rows read so far. */
        private int nvisited = 0;
        
        /** The next {@link TPS} to be returned. */
        private TPS current = null;
        
        /**
         * The prefix key for the {@link #current} {@link TPS}. This is
         * identified when we read the first tuple from the logical row. Doing
         * it this way means that we do not need to encode the primary key and
         * avoids a dependency on a correctly configured {@link IKeyBuilder}.
         */
        private byte[] prefix;
        
        /**
         * Each visited tuple corresponds to a logical row. The key for the
         * tuple is the {schema,primaryKey} for that logical row. The value is
         * the serialized {@link TPS} object for the corresponding logical row.
         * 
         * @see ITuple#getObject()
         */
        public ITuple<TPS> next() {
            
            if (!hasNext())
                throw new NoSuchElementException();
            
            assert current != null;

            final TPS tps = current;
            
            current = null;
            
            final AbstractTuple<TPS> tuple = new AbstractTuple<TPS>(flags) {

                /**
                 * @todo This can't be implemented since the tuples may have
                 *       come from different backing AbstractBTree's in the
                 *       view. If blob references are to be supported they will
                 *       have to be transformed during the atomic row read so
                 *       that the incorporate the source index from which the
                 *       block can be read.
                 *       <p>
                 *       There are some notes on introducing blob support into
                 *       the {@link SparseRowStore} on that class. Basically
                 *       there would need to be a blob value type and it would
                 *       have to carry the UUID for the store from which the
                 *       blob could be read as part of the property value stored
                 *       in a TPS.
                 */
                @Override
                public int getSourceIndex() {

                    throw new UnsupportedOperationException();
                    
                }
                
                protected ITupleSerializer getTupleSerializer() {
                    
                    return tupleSer;
                    
                }
                
            };
            
            tuple.copyTuple(prefix, SerializerUtil.serialize(tps));
            
            // visited another logical row.
            nvisited++;
            
            return tuple;
            
        }

        /**
         * When {@link #current} is <code>null</code> this scans ahead until
         * it reaches the first tuple that does not belong to the current
         * logical row. The logical row is assembled from the visited tuples as
         * they are read from the source iterator.
         */
        public boolean hasNext() {
            
            if (current != null) {

                // the next logical row is ready.
                return true;
                
            }

            if (nvisited >= capacity) {
                
                // the iterator is at its capacity.
                return false;
                
            }
            
            // Result set object.
            final TPS tps = new TPS(schema, timestamp);

            // clear the prefix - it serves as a flag for this loop.
            prefix = null;
            
            while (src.hasNext()) {

                final ITuple tuple = src.next();

                final byte[] prefix = new KeyDecoder(tuple.getKey()).getPrefix();

                if (this.prefix == null) {

                    // start of a new logical row.
                    this.prefix = prefix;

                } else if (!BytesUtil.bytesEqual(this.prefix, prefix)) {

                    // end of the current logical row.
                    break;

                }
                
                // extract a property value from the tuple.
                handleTuple(tps, tuple);

            }

            if (prefix != null) {
                
                /*
                 * Found at least one tuple belonging to a logical row.
                 * 
                 * Note: The logical row MAY be empty depending on the
                 * INameFilter and timestamp, but we will visit it anyway.
                 */
                
                current = tps;
                
                return true;
                
            }

            // Nothing left.
            return false;

        }

        public void remove() {

            // TODO look at the remove semantics for the sparse row store.
            
        }
        
        /**
         * Extracts a property value from the tuple and adds it to the logical
         * row if the timestamp and optional {@link INameFilter} are satisified
         * for the tuple.
         * 
         * @param tps
         *            The logical row.
         * @param tuple
         *            The tuple.
         */
        private void handleTuple(TPS tps, ITuple tuple) {

            assert tps != null;
            
            assert tuple != null;
            
            final byte[] key = tuple.getKey();

            final byte[] val = tuple.getValue();

            /*
             * Decode the key so that we can get the column name. We have the
             * advantage of knowing the last byte in the primary key. Since the
             * fromKey was formed as [schema][primaryKey], the length of the
             * fromKey is the index of the 1st byte in the column name.
             */

            final KeyDecoder keyDecoder = new KeyDecoder(key);

            // The column name.
            final String col = keyDecoder.getColumnName();

            if (nameFilter != null && !nameFilter.accept(col)) {

                // Skip property names that have been filtered out.

                if (log.isDebugEnabled()) {

                    log.debug("Skipping property: name=" + col);

                }

                return;

            }

            /*
             * Skip column values having a timestamp strictly greater than the
             * given value.
             */
            final long columnValueTimestamp = keyDecoder.getTimestamp();
            {

                if (columnValueTimestamp > timestamp) {

                    if (log.isDebugEnabled()) {

                        log.debug("Ignoring newer revision: col=" + col
                                + ", timestamp=" + columnValueTimestamp);

                    }

                    return;

                }

            }

            /*
             * Decode the value. A [null] indicates a deleted property
             * value.
             */

            final Object v = ValueType.decode(val);

            /*
             * Add to the representation of the row.
             */

            tps.set(col, columnValueTimestamp, v);

            if (log.isInfoEnabled())
                log.info("Read: name=" + col + ", timestamp="
                        + columnValueTimestamp + ", value=" + v);

        }

        /**
         * Always returns <code>true</code>.
         */
        @Override
        protected boolean isValid(ITuple tuple) {
            
            return true;
            
        }

    }
    
    /**
     * Helper class for <em>de-serializing</em> logical rows from an
     * {@link AtomicRowIterator2}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TPSTupleSerializer implements ITupleSerializer {

        private static final long serialVersionUID = -2467715806323261423L;

        public static transient final ITupleSerializer INSTANCE = new TPSTupleSerializer(); 
        
        /**
         * De-serializator ctor.
         */
        public TPSTupleSerializer() {
            
        }
        
        public TPS deserialize(ITuple tuple) {

            return (TPS) SerializerUtil.deserialize(tuple.getValueStream());
            
        }

        /**
         * You can get the {@link Schema} and the primary key from
         * {@link #deserialize(ITuple)}.
         * 
         * @throws UnsupportedOperationException
         *             always.
         */
        public Object deserializeKey(ITuple tuple) {

            throw new UnsupportedOperationException();
            
        }

        /**
         * This method is not used since we do not store {@link TPS} objects
         * directly in a {@link BTree}.
         * 
         * @throws UnsupportedOperationException
         *             always.
         */
        public byte[] serializeKey(Object obj) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * This method is not used since we do not store {@link TPS} objects
         * directly in a {@link BTree}.
         * 
         * @throws UnsupportedOperationException
         *             always.
         */
        public byte[] serializeVal(Object obj) {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
}
