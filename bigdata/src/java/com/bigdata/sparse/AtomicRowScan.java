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
 * Created on May 29, 2008
 */

package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractChunkedTupleIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.service.ClientIndexView;

/**
 * The {@link AtomicRowScan} identifes the set of tuples in the index that
 * corresponds to a logical row (having the same primary key) as an atomic
 * operation. It is capable of scanning all logical rows within a key range,
 * where the key range identifies a span of primary keys. Individual rows are
 * always read atomically. When multiple rows appear in the same index partition
 * they MAY be read atomically. The actual #of rows that will be read atomically
 * from a given index partition is a function of both the #of matching rows in
 * the index partition and the (target) buffer capacity for a request.
 * <P>
 * This depends on two things: (1) index partition split points must be choosen
 * such that the primary key is never split across an index partition boundary -
 * this is necessary since {@link ITx#UNISOLATED} operations across index
 * partitions are NOT atomic. (2) the scan must identify the end of the logical
 * row and return only at complete logical row boundaries. Those boundaries
 * occur either when the next observed key in the index partition belongs to a
 * different logical row (the primary key is different) or when the end of the
 * index partition is reached by the scan (since we force index partition splits
 * to occur between logical rows).
 * 
 * FIXME In order for this method to correctly progress through an index
 * partition in a sequence of requests where at most N logical rows are
 * identified by request there must be an abstraction that can be used to
 * formulate the next request. The {@link AbstractChunkedTupleIterator} handles
 * this for {@link ITupleIterator} but there is not a general purpose approach
 * to handling this problem. As a result the actual behavior of this procedure
 * will be to read up to N logical rows from each index partition in order, but
 * it will fail to read more than N logical rows from an index partition when
 * there are in fact more than N logical rows in that index partition because it
 * lacks any sense of a "continuation" query for the index partition.
 * <p>
 * The broader issue is whether the {@link ITupleIterator} should be generalized
 * to handle abstractions such as prefix scans and atomic row scans or whether
 * an abstract can be put into place that makes it easy to write robust iterator
 * based constructs using continuation queries. Note that {@link ITupleIterator}
 * provides a fairly rich set of semantics and that re-creating those semantics
 * for different iterator constructs could be time consuming.
 * 
 * FIXME implement logical row scan. This may require a modification to how we
 * do key range scans since each logical row read needs to be atomic. While rows
 * will not be split across index partitions, it is possible that the iterator
 * would otherwise stop when it had N index entries rather than N logical rows,
 * thereby requiring a restart of the iterator from the successor of the last
 * fully read logical row. One way to handle that is to make the limit a
 * function that can be interpreted on the data service in terms of index
 * entries or some other abstraction -- in this case the #of logical rows. (A
 * logical row ends when the primary key changes.) Note: With this approach the
 * response is the set of tuples matching the filter and the client reassembles
 * those tuples into logical rows.
 * 
 * @todo The atomic row scan COULD be parallelized across the index partitions
 *       if you do not care about the order in which the results appear, but
 *       continuation queries will still be serialized against a given index
 *       partition.
 *       <p>
 *       The code would have to be modified to support this. Eg, by making the
 *       class abstract and then having two concrete implementations where only
 *       one implements {@link IParallelizableIndexProcedure}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link AtomicRowFilter}
 */
public class AtomicRowScan extends AbstractKeyRangeIndexProcedure implements Externalizable {

    private static final long serialVersionUID = 4348822274424762674L;

    protected static final Logger log = Logger.getLogger(AtomicRowScan.class);
    
    /**
     * The default upper bound on the #of logical rows that will be read
     * atomically from a single index partition.
     */
    public static transient final int DEFAULT_CAPACITY = 1000;
    
    /** The #of logical rows to include in each result. */
    protected int capacity;
    protected Schema schema;
    protected long timestamp;
    protected INameFilter filter;

    /**
     * The scan is read-only (it does not support atomic delete of the visited
     * logical rows or other mutation operations on those rows during the scan).
     */
    public final boolean isReadOnly() {
        
        return true;
        
    }
    
    /**
     * De-serialization ctor.
     */
    public AtomicRowScan() {
        
    }

    /**
     * Constructor for an atomic logical row scan operation.
     * 
     * @param schema
     *            The schema governing the property set.
     * @param fromKey
     *            The inclusive lower bound for the primary key. When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The exclusive upper bound for the primary key. When
     *            <code>null</code> there is no upper bound.
     * @param timestamp
     *            Either a timestamp to obtain property values whose timestamp
     *            does not exceed <i>timestamp</i> -or-
     *            {@link SparseRowStore#MAX_TIMESTAMP} to obtain the most recent
     *            value for each matched property.
     * @param filter
     *            An optional filter used to restrict the properties whose
     *            values will be returned.
     * @param capacity
     *            The target #of logical rows to return per request. This
     *            influences how many logical rows will be collected during a
     *            scan of a single index partition. A value of zero is
     *            interpreted as a default. The maximum #of logical rows that
     *            will be returned is bounded by the #of logical rows which
     *            satisfy the key range constraint in a given index partition.
     */
    public AtomicRowScan(Schema schema, final byte[] fromKey,
            final byte[] toKey, long timestamp, INameFilter filter, int capacity) {

        super(fromKey,toKey);
        
        if (schema == null)
            throw new IllegalArgumentException("No schema");

        if (capacity < 0)
            throw new IllegalArgumentException();

        if (capacity == 0) {

            capacity = DEFAULT_CAPACITY;

        }

        this.capacity = capacity;
        
        this.schema = schema;

        this.timestamp = timestamp;

        this.filter = filter;

    }

    /**
     * Reads zero or more logical rows, applying the timestamp and the optional
     * filter.
     * 
     * @return A serialized representation of those rows together with either
     *         the <i>fromKey</i> from which the scan should continue or
     *         <code>null</code> iff the procedure has scanned all logical
     *         rows in the specified key range (i.e., it has observed a primary
     *         key that is GTE <i>toKey</i>). Note that the other termination
     *         condition is when there are no more index partitions spanned by
     *         the key range, which is handled by the {@link ClientIndexView}.
     */
    public TPSList apply(IIndex ndx) {
     
        // The ordered set of logical rows populated by this request.
        final List<TPS> rows = new LinkedList<TPS>();
        
        while (true) {

            /*
             * Advance to the next logical row in the index.
             */
            fromKey = findNextRow(ndx, fromKey);

            if (fromKey == null) {

                /*
                 * There are no more logical rows in this index partition.
                 */
                break;

            }

            // read the logical row from the index.
            final TPS tps = AtomicRowRead.atomicRead(ndx, fromKey, schema,
                    timestamp, filter);

            /*
             * Note: tps SHOULD NOT be null since null is reserved to indicate
             * that there was NO data for a given primary key (not just no
             * property values matching the optional constraints) and we are
             * scanning with a [fromKey] that is known to exist.
             */
            assert tps != null;

            // add to the result set.
            rows.add(tps);
            
            /*
             * This is the next possible key with which the following logical
             * row could start. If we restart this loop we will scan the index
             * starting at this key until we find the actual key for the next
             * logical row. Otherwise this is where the scan should pick up if
             * we have reached the capacity on the current request.
             */
            fromKey = SuccessorUtil.successor(fromKey);
            
            if (rows.size() >= capacity) {

                /*
                 * Stop since we have reached the capacity for this request.
                 */
                
                break;
                
            }
            
        }

        if (log.isInfoEnabled()) {

            log.info("Read " + rows.size()+" rows: capacity="+capacity);
        
        }

        /*
         * Return both the key for continuing the scan and the ordered set of
         * logical rows identified by this request.
         */

        return new TPSList(fromKey, rows.toArray(new TPS[rows.size()]));
        
    }

    /**
     * Scans the index starting at the specified lower bound and reports the
     * first key GTE to that key yet LE {@link #getToKey()}.
     * 
     * @param fromKey
     * 
     * @return The first key GTE the given key -or- <code>null</code> iff
     *         there are NO keys remaining in the index partition GTE the given
     *         key.
     */
    protected byte[] findNextRow(final IIndex ndx, final byte[] fromKey) {
        
        final ITupleIterator itr = ndx.rangeIterator(fromKey, getToKey(),
                1/* capacity */, IRangeQuery.KEYS, null/* filter */);

        while (itr.hasNext()) {

            final byte[] key = itr.next().getKey();
            
            final KeyDecoder decoded = new KeyDecoder(key);

            // extract just the schema and the primary key.
            return decoded.getPrefix();

        }

        return null;
        
    }
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final short version = in.readShort();

        if (version != VERSION0) {

            throw new IOException("Unknown version=" + version);

        }

        capacity = in.readInt();
        
        schema = (Schema) in.readObject();

        timestamp = in.readLong();

        filter = (INameFilter) in.readObject();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeShort(VERSION0);

        out.writeInt(capacity);
        
        out.writeObject(schema);

        out.writeLong(timestamp);

        out.writeObject(filter);

    }

    private final static transient short VERSION0 = 0x0;

    /**
     * Zero or more {@link TPS} instances and some metadata that indicates where
     * to continue the scan or whether the scan is known to be terminated.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TPSList implements Externalizable {
       
        private static final long serialVersionUID = -7319479325177575731L;

        /**
         * The next key from which the scan should be continued -or-
         * <code>null</code> if the scan is known to be terminated because we
         * have observed that the next available key is GTE to the optional
         * upper bound for the scan.
         */
        private byte[] nextKey;

        /**
         * The ordered array of the logical rows identified by the scan.
         */
        private TPS[] a;
        
        /**
         * Iterator visits the ordered array of the logical rows identified by
         * the scan.
         */
        public Iterator<? extends ITPS> iterator() {
            
            return Arrays.asList(a).iterator();
            
        }

        /**
         * De-serialization ctor.
         */
        public TPSList() {
            
        }

        /**
         * 
         * @param nextKey
         *            The next key from which the scan should continue on this
         *            index partition -or- <code>null</code> if this index
         *            partition has been exhausted.
         * @param a
         *            The ordered set of logical rows identified by the request.
         */
        public TPSList(byte[] nextKey, TPS[] a) {
            
            if (a == null)
                throw new IllegalArgumentException();
            
            this.nextKey = nextKey;
            
            this.a = a;
            
        }
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            
            final int version = in.readInt();
            
            if (version != VERSION0)
                throw new UnsupportedOperationException("unknown version="
                        + version);

            final int nextKeyLen = in.readInt();
            
            if(nextKeyLen==-1) {
                
                nextKey = null;
                
            } else {
                
                nextKey = new byte[nextKeyLen];
                
                in.readFully(nextKey);
                
            }
            
            final int nfound = in.readInt();
            
            a = new TPS[nfound];
            
            for(int i=0; i<nfound; i++) {
                
                a[i] = (TPS) in.readObject();
                
            }
            
        }
        
        public void writeExternal(ObjectOutput out) throws IOException {
        
            out.writeInt(VERSION0);

            out.write(nextKey == null ? -1 : nextKey.length);
            
            if (nextKey != null)
                out.write(nextKey);
            
            out.writeInt(a.length);
            
            for(TPS tps : a) {
                
                out.writeObject(tps);
                
            }
            
        }

        private static int VERSION0 = 0x0;
        
    }
    
}
