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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IDataSerializer.DefaultDataSerializer;
import com.bigdata.btree.IDataSerializer.SimplePrefixSerializer;
import com.bigdata.btree.KeyBuilder.StrengthEnum;
import com.bigdata.sparse.AtomicRead;
import com.bigdata.sparse.AtomicRowScan;
import com.bigdata.sparse.INameFilter;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.KeyDecoder;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.sparse.TPS;
import com.bigdata.sparse.TPSTupleSerializer;
import com.bigdata.sparse.ValueType;
import com.bigdata.sparse.TPS.TPV;

/**
 * Abstract base class for filter or transforming {@link ITupleIterator}s. This
 * class provides a general purpose mechanism that may be used to layer the
 * semantics of an {@link ITupleIterator}. The primary use cases are filtering
 * of tuples based on their key and/or value and aggregation of atomic reads to
 * higher level data structures such as the {@link AtomicRowScan}.
 * 
 * FIXME The {@link AbstractTupleFilterator} is now a full
 * {@link ITupleIterator} and it WRAPS the base iterator requested by the
 * caller. It can be used directly when you know that you are operating on a
 * local index or you can use the {@link AbstractTupleFilteratorConstructor}
 * when you need to describe the filter on the client and have the wrapper setup
 * for you on the server. (It would be nice if we could just construct the
 * iterator have have it get serialized out correctly with the appropriate
 * splits but that seems to be modestly complex).
 * 
 * FIXME Modify {@link IRangeQuery} to change the type of the "filter" argument
 * to a factory object for an {@link AbstractTupleFilterator}, e.g.,
 * {@link AbstractTupleFilteratorConstructor} so that we can describe the filter
 * on the client and have it instantiated with its state on the server.
 * 
 * FIXME Add a variant to {@link IRangeQuery} that accepts an array of keys and
 * maps a caller supplied {@link AbstractTupleFilterator} across those keys.
 * This will be used for the prefix scan.
 * 
 * FIXME The distinct term scan can not be written just yet. It needs to advance
 * the {@link ITupleIterator} to a key formed from the successor of the current
 * key. In order to support that {@link LeafTupleIterator} needs to support the
 * "gotoKey" (as long as it is within the half-open range) and an
 * {@link AbstractTupleFilterator} can then be written that advances the source
 * iterator. Since it will all be "just an iterator" the client code will map it
 * across the index partitions without change. (Note: this does suggest that
 * skipping backwards could cause the client code to not terminate if the
 * filterator has its logic wrong).
 * 
 * FIXME There is a strong relationship between a byte[][] keys iterator such as
 * the {@link CompletionScan} and an unrolled JOIN operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTupleFilterator<E> implements ITupleIterator<E> {

    protected static final Logger log = Logger.getLogger(AbstractTupleFilterator.class);
    
    protected final ITupleIterator<E> src;
    
    private ITuple<E> current = null;
    
    protected AbstractTupleFilterator(ITupleIterator<E> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public ITuple<E> next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        assert current != null;

        final ITuple<E> t = current;

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

            final ITuple<E> tuple = src.next();

            if (isValid(tuple)) {

                current = tuple;

                return true;

            }

        }

        return false;

    }

    public void remove() {

        src.remove();

    }

    /**
     * Return true iff the tuple should be visited.
     * 
     * @param tuple
     *            The tuple.
     */
    abstract protected boolean isValid(ITuple<E> tuple);
    
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
     * @param E
     *            The generic type for the objects materialized from the source
     *            tuples.
     * @param F
     *            The generic type for the objects that can be materialized from
     *            the output tuples.
     *            
     * FIXME make sure that the various compression and serialization handlers
     * specified here are imposed by the {@link ResultSet} which MUST notice
     * this class and use its properties in preference to those available from
     * the {@link IndexMetadata}
     */
    abstract protected static class AbstractTransformingTupleIterator<E,F> implements ITupleIterator<F> {

        /** The flags specified for the source iterator. */
        final protected int flags;

        /** The source iterator. */
        final protected ITupleIterator<E> src;
        
        /** The compression provider for the key[]. */
        final protected IDataSerializer leafKeySer;
        
        /** The compression provider for the value[]. */
        final protected IDataSerializer leafValSer;

        /** The serialization provider for the transformed tuples. */
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
         *            The serialization provider for the transformed tuples.
         */
        protected AbstractTransformingTupleIterator(ITupleIterator<E> src,
                int flags, IDataSerializer leafKeySer,
                IDataSerializer leafValSer, ITupleSerializer tupleSer) {

            if (src == null)
                throw new IllegalArgumentException();

            if (leafKeySer == null)
                throw new IllegalArgumentException();

            if (leafValSer == null)
                throw new IllegalArgumentException();

            if (tupleSer == null)
                throw new IllegalArgumentException();

            this.flags = flags;
            
            this.src = src;
            
            this.leafKeySer = leafKeySer;

            this.leafValSer = leafValSer;
            
            this.tupleSer = tupleSer;

        }

    }
    
    /**
     * Transforms an {@link ITupleIterator} reading directly on an
     * {@link IIndex} backing a {@link SparseRowStore} into an
     * {@link ITupleIterator} visiting logical {@link ITPS} rows.
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
    public static class AtomicRowIterator2 extends AbstractTransformingTupleIterator<TPV, TPS> {

        protected static final Logger log = Logger.getLogger(AtomicRowIterator2.class);
        
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
        protected AtomicRowIterator2(ITupleIterator<TPV> src, int capacity, int flags,
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
                 *       <p>
                 *       Currently, blobs are stored in the same index as the
                 *       blob reference. In order to allow blobs to be stored in
                 *       a different index the name of the scale out index would
                 *       have to be in the blob reference.
                 */
                public int getSourceIndex() {

                    throw new UnsupportedOperationException();
                    
                }
                
                public ITupleSerializer getTupleSerializer() {
                    
                    return tupleSer;
                    
                }
                
            };
            
            tuple.copyTuple(prefix, tupleSer.serializeVal(tps));
            
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

                final ITuple<TPV> tuple = src.next();

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
            throw new UnsupportedOperationException(); 
            
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

            // Decode the key so that we can get the column name.
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

    }
    
    /**
     * A filter that removes the tuples that it visits from the source iterator.
     * <p>
     * Note: This filter may be used to cause tuples to be atomically deleted on
     * the server.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Removerator<E> implements ITupleIterator<E> {

        private final ITupleIterator<E> src;
        
        /**
         * @param src
         *            The source iterator.
         */
        protected Removerator(ITupleIterator<E> src) {

            if (src == null)
                throw new IllegalArgumentException();

            this.src = src;
            
        }

        public ITuple<E> next() {

            final ITuple<E> t = src.next();

            src.remove();
            
            return t;
            
        }

        public boolean hasNext() {

            return src.hasNext();
            
        }

        /**
         * NOP - the visited tuples are ALWAYS removed by {@link #next()}.
         */
        public void remove() {
          
        }
        
    }

    /**
     * <p>
     * Iterator-based scan visits all tuples having a shared prefix. The
     * iterator accepts a key or an array of keys that define the key prefix(s)
     * whose completions will be visited. It efficiently forms the successor of
     * each key prefix, performs a key-range scan of the key prefix, and (if
     * more than one key prefix is given), seeks to the start of the next
     * key-range scan.
     * </p>
     * <h4>WARNING</h4>
     * <p>
     * <strong>The prefix keys MUST be formed with
     * {@link StrengthEnum#Identical}. This is necessary in order to match all
     * keys in the index since it causes the secondary characteristics to NOT be
     * included in the prefix key even if they are present in the keys in the
     * index.</strong> Using other {@link StrengthEnum}s will result in
     * secondary characteristics being encoded by additional bytes appended to
     * the key. This will result in scan matching ONLY the given prefix key(s)
     * and matching nothing if those prefix keys are not actually present in the
     * index.
     * </p>
     * <p>
     * For example, the Unicode text "Bryan" is encoded as the <em>unsigned</em>
     * byte[]
     * </p>
     * 
     * <pre>
     * [43, 75, 89, 41, 67]
     * </pre>
     * 
     * <p>
     * at PRIMARY strength but as the <em>unsigned</em> byte[]
     * </p>
     * 
     * <pre>
     * [43, 75, 89, 41, 67, 1, 9, 1, 143, 8]
     * </pre>
     * 
     * <p>
     * at IDENTICAL strength. The additional bytes for the IDENTICAL strength
     * reflect the Locale specific Unicode sort key encoding of secondary
     * characteristics such as case. The successor of the PRIMARY strength
     * byte[] is
     * </p>
     * 
     * <pre>
     * [43, 75, 89, 41, 68]
     * </pre>
     * 
     * <p>
     * (one was added to the last byte) which spans all keys of interest.
     * However the successor of the IDENTICAL strength byte[] would
     * </p>
     * 
     * <pre>
     * [43, 75, 89, 41, 67, 1, 9, 1, 143, 9]
     * </pre>
     * 
     * <p>
     * and would ONLY span the single tuple whose key was "Bryan".
     * </p>
     * <p>
     * You can form an appropriate {@link IKeyBuilder} for the prefix keys using
     * </p>
     * 
     * <pre>
     * Properties properties = new Properties();
     * 
     * properties.setProperty(KeyBuilder.Options.STRENGTH, StrengthEnum.Primary
     *         .toString());
     * 
     * prefixKeyBuilder = KeyBuilder.newUnicodeInstance(properties);
     * </pre>
     * 
     * FIXME Support for remote and local processes.
     * 
     * FIXME Only pass the relevant elements of keyPrefix to any given index
     * partition. It is possible that an element spans the end of an index
     * partition, in which case the scan must resume with the next partition.
     * There is no real way to know this without testing the next partition....
     * 
     * FIXME Note: It is NOT trivial to define filter that may be used to accept
     * only keys that extend the prefix on a caller-defined boundary (e.g.,
     * corresponding to the encoding of a whitespace or word break). There are
     * two issues: (1) the keys are encoded so the filter needs to recognize the
     * byte(s) in the Unicode sort key that correspond to, e.g., the work
     * boundary. (2) the keys may have been encoded with secondary
     * characteristics, in which case the boundary will not begin immediately
     * after the prefix.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see TestCompletionScan
     */
    public static class CompletionScan<E> implements ITupleIterator<E> {

        /**
         * The source iterator. The lower bound for the source iterator should
         * be the first key prefix. The upper bound should be the fixed length
         * successor of the last key prefix (formed by adding one bit, not by
         * appending a <code>nul</code> byte).
         */
        protected final ITupleCursor<E> src;
        
        /** The array of key prefixes to be scanned. */
        protected final byte[][] keyPrefix;

        /**
         * The index of the key prefix that is currently being scanned. The
         * entire scan is complete when index == keyPrefix.length.
         */
        private int index = 0;

        /**
         * The exclusive upper bound. This is updated each time we begin to scan
         * another key prefix.
         */
        protected byte[] toKey;
        
        /** The current tuple. */
        private ITuple<E> current = null;

        /**
         * Completion scan with a single prefix. The iterator will visit all
         * tuples having the given key prefix.
         * 
         * @param src
         *            The source iterator.
         * @param keyPrefix
         *            An unsigned byte[] containing a key prefix.
         */
        public CompletionScan(ITupleCursor<E> src, byte[] keyPrefix) {

            this(src, new byte[][] { keyPrefix });
            
        }
        
        /**
         * Completion scan with an array of key prefixes. The iterator will
         * visit all tuples having the first key prefix, then all tuples having
         * the next key prefix, etc. until all key prefixes have been evaluated.
         * 
         * @param src
         *            The source iterator (must specify at least
         *            {@link IRangeQuery#KEYS}, AND {@link IRangeQuery#CURSOR}).
         * @param keyPrefix
         *            An array of unsigned byte prefixes (the elements of the
         *            array MUST be presented in sorted order).
         */
        public CompletionScan(ITupleCursor<E> src, byte[][] keyPrefix) {
            
            if (src == null)
                throw new IllegalArgumentException();

            if (keyPrefix == null)
                throw new IllegalArgumentException();

            if (keyPrefix.length == 0)
                throw new IllegalArgumentException();
            
            this.src = src;
            
            this.keyPrefix = keyPrefix;

            this.index = 0;
            
            nextPrefix();
            
        }
        
        public boolean hasNext() {

            if (current != null)
                return true;

            /*
             * Find the next tuple having the same prefix.
             */
            while (src.hasNext()) {

                final ITuple<E> tuple = src.next();

                final byte[] key = tuple.getKey();
                
                if (BytesUtil.compareBytes(key, toKey) >= 0) {
                    
                    if (log.isInfoEnabled())
                        log.info("Scanned beyond prefix: toKey="
                                + BytesUtil.toString(toKey) + ", tuple=" + tuple);
                    
                    if (index + 1 < keyPrefix.length) {

                        // next prefix.
                        index++;
                        
                        nextPrefix();
                        
                        if (current != null) {

                            // found an exact prefix match.
                            return true;
                            
                        }
                        
                        continue;
                        
                    }
                        
                    log.info("No more prefixes.");
                    
                    return false;
                        
                }
                
                current = tuple;

                // found another tuple that is a completion of the current prefix.
                return true;

            }

            // no more tuples (at least in this index partition).
            
            log.info("No more tuples.");
            
            return false;

        }

        /**
         * Start a sub-scan of the key prefix at the current {@link #index}.
         */
        protected void nextPrefix() {
            
            final byte[] prefix = keyPrefix[index];
            
            // make a note of the exclusive upper bound for that prefix.
            toKey = SuccessorUtil.successor(keyPrefix[index].clone());

            // seek to the inclusive lower bound for that key prefix.
            src.seek(prefix);
            
            /*
             * Note: if we seek to a key that has a visitable tuple then that
             * will be the next tuple to be returned.
             */
            current = src.tuple();
            
            if (log.isInfoEnabled()) {

                log.info("index=" + index + ", prefix="
                        + BytesUtil.toString(prefix) + ", current=" + current);
                
            }
            
        }
        
        public ITuple<E> next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            assert current != null;

            final ITuple<E> t = current;

            current = null;

            return t;

        }

        public void remove() {

            src.remove();

        }

    }

    /**
     * Delegates all operations to another {@link ITupleCursor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class DelegateTupleCursor<E> implements ITupleCursor<E>{    
        
        final private ITupleCursor<E> src;
        
        public DelegateTupleCursor(ITupleCursor<E> src) {
            
            if (src == null)
                throw new IllegalArgumentException();

            this.src = src;
        
        }

        public byte[] currentKey() {
            return src.currentKey();
        }

        public ITuple<E> first() {
            return src.first();
        }

        public byte[] getFromKey() {
            return src.getFromKey();
        }

        public IIndex getIndex() {
            return src.getIndex();
        }

        public byte[] getToKey() {
            return src.getToKey();
        }

        public boolean hasNext() {
            return src.hasNext();
        }

        public boolean hasPrior() {
            return src.hasPrior();
        }

        public boolean isCursorPositionDefined() {
            return src.isCursorPositionDefined();
        }

        public boolean isDeletedTupleVisitor() {
            return src.isDeletedTupleVisitor();
        }

        public ITuple<E> last() {
            return src.last();
        }

        public ITuple<E> next() {
            return src.next();
        }

        public ITuple<E> nextTuple() {
            return src.nextTuple();
        }

        public ITuple<E> prior() {
            return src.prior();
        }

        public ITuple<E> priorTuple() {
            return src.priorTuple();
        }

        public void remove() {
            src.remove();
        }

        public ITuple<E> seek(byte[] key) {
            return src.seek(key);
        }

        public ITuple<E> seek(Object key) {
            return src.seek(key);
        }

        public ITuple<E> tuple() {
            return src.tuple();
        }
        
        
    }

    /**
     * Wraps an {@link ITupleIterator} as an {@link ITupleCursor}. Methods that
     * are not declared by {@link ITupleIterator} will throw an
     * {@link UnsupportedOperationException}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class WrappedTupleIterator<E> implements ITupleCursor<E> {

        final private ITupleIterator<E> src;

        public WrappedTupleIterator(ITupleIterator<E> src) {

            if (src == null)
                throw new IllegalArgumentException();

            this.src = src;

        }

        public boolean hasNext() {
            return src.hasNext();
        }

        public ITuple<E> next() {
            return src.next();
        }

        public void remove() {
            src.remove();
        }

        public byte[] currentKey() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> first() {
            throw new UnsupportedOperationException();
        }

        public byte[] getFromKey() {
            throw new UnsupportedOperationException();
        }

        public IIndex getIndex() {
            throw new UnsupportedOperationException();
        }

        public byte[] getToKey() {
            throw new UnsupportedOperationException();
        }

        public boolean hasPrior() {
            throw new UnsupportedOperationException();
        }

        public boolean isCursorPositionDefined() {
            throw new UnsupportedOperationException();
        }

        public boolean isDeletedTupleVisitor() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> last() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> nextTuple() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> prior() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> priorTuple() {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> seek(byte[] key) {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> seek(Object key) {
            throw new UnsupportedOperationException();
        }

        public ITuple<E> tuple() {
            throw new UnsupportedOperationException();
        }

    }

}
