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
package com.bigdata.btree;

import it.unimi.dsi.mg4j.io.InputBitStream;
import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Iterator;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractTupleFilterator.AbstractTransformingTupleIterator;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.IDataService;

/**
 * An object used to stream key scan results back to the client.
 * 
 * FIXME support {@link AbstractTransformingTupleIterator} - must apply its
 * compression and serialization handlers and serialize them for the client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResultSet implements Externalizable {

    private static final long serialVersionUID = -390738836663476282L;

    protected static final Logger log = Logger.getLogger(ResultSet.class);

//  protected int rangeCountx;

    /** true iff keys were requested. */
    private boolean sendKeys;

    /** true iff values were requested. */
    private boolean sendVals;
    
    private int ntuples;

    private boolean exhausted;

    private byte[] lastKey;

    private RandomAccessByteArray keys;

    private RandomAccessByteArray vals;

    private long[] versionTimestamps;
    
    private byte[] deleteMarkers;

    private byte[] sourceIndices;
    
    private IResourceMetadata[] sources;
    
    private long commitTime;
    
    /**
     * Set automatically based on the {@link IndexMetadata}.
     */
    private IDataSerializer leafKeySerializer;

    /**
     * Set automatically based on the {@link IndexMetadata}.
     */
    private IDataSerializer leafValSerializer;
    
    /**
     * Set automatically based on the {@link IndexMetadata}.
     */
    private ITupleSerializer tupleSerializer;
    
//    /**
//     * Total #of key-value pairs within the key range (approximate).
//     */
//    public int getRangeCount() {
//       
//        return rangeCount;
//        
//    }

    /**
     * Actual #of key-value pairs in the {@link ResultSet}
     */
    final public int getNumTuples() {
        
        return ntuples;
        
    }

    /**
     * True iff the iterator exhausted the available keys such that no more
     * results would be available if you formed the successor of the
     * {@link #lastKey}.
     */
    final public boolean isExhausted() {
        
        return exhausted;
        
    }

    /**
     * The last key visited by the iterator <em>regardless</em> of the
     * filter imposed -or- <code>null</code> iff no keys were visited by
     * the iterator for the specified key range.
     * 
     * @see #successor()
     */
    final public byte[] getLastKey() {
        
        return lastKey;
        
    }

    /**
     * The next key that should be used to retrieve keys and/or values starting
     * from the first possible successor of the {@link #getLastKey()} visited by
     * the iterator in this operation (the successor is formed by appending a
     * <code>nul</code> byte to the {@link #getLastKey()}).
     * 
     * @return The successor of {@link #getLastKey()} -or- <code>null</code>
     *         iff the iterator exhausted the available keys.
     * 
     * @exception UnsupportedOperationException
     *                if the {@link #lastKey} is <code>null</code>.
     */
    final public byte[] successor() {

        if (lastKey == null)
            throw new UnsupportedOperationException();

        return BytesUtil.successor(lastKey);

    }

    /**
     * The {@link IDataSerializer} used to (de-)compress the {@link #keys}.
     */
    final public IDataSerializer getLeafKeySerializer() {
        
        return leafKeySerializer;
        
    }
    
    /**
     * The {@link IDataSerializer} used to (de-)compress the {@link #vals}.
     */
    final public IDataSerializer getLeafValueSerializer() {
        
        return leafValSerializer;
        
    }
    
    /**
     * The {@link ITupleSerializer} that may be used to de-serialize the tuples
     * in the {@link ResultSet}.
     */
    final public ITupleSerializer getTupleSerializer() {
        
        return tupleSerializer;
        
    }
    
    /**
     * Return the keys.
     * 
     * @throws IllegalStateException
     *             if the keys were not retrieved.
     */
    final public IRandomAccessByteArray getKeys() {
        
        if (keys == null)
            throw new IllegalStateException();
        
        return keys;
        
    }
    
    /**
     * Return the values.
     * 
     * @throws IllegalStateException
     *             if the values were not retrieved.
     */
    final public IRandomAccessByteArray getValues() {
        
        if (vals == null)
            throw new IllegalStateException();
        
        return vals;
        
    }
    
    /**
     * Return the key at the specified index.
     * 
     * @param index
     *            The index of the key to be returned.
     * 
     * @see #getKeys()
     * 
     * @throws IllegalStateException
     *             if the keys were not retrieved.
     */
    final public byte[] getKey(int index) {
        
        if (keys == null)
            throw new IllegalStateException();
        
        return keys.getKey(index);
        
    }

    /**
     * Return the value at the specified index.
     * 
     * @param index
     *            The index of the value to be returned.
     * 
     * @see #getValues()
     * 
     * @throws IllegalStateException
     *             if the values were not retrieved.
     */
    final public byte[] getValue(int index) {
        
        if (vals == null)
            throw new IllegalStateException();
        
        return vals.getKey(index);
        
    }

    /**
     * The visited version timestamps iff the index maintains version
     * timestamps.
     * 
     * @return The version timestamps -or- <code>null</code> iff the index
     *         does not maintain version timestamps.
     */
    final public long[] getVersionTimestamps() {
        
        return versionTimestamps;
        
    }
    
    /**
     * The visited delete markers iff the the index maintains delete markers.
     * The bytes are coded as ONE (1) is true and ZERO (0) is false.
     * 
     * @return The delete markers -or- <code>null</code> iff the index does
     *         not maintain delete markers.
     */
    final public byte[] getDeleteMarkers() {
        
        return deleteMarkers;
        
    }

    /**
     * The values returned by {@link ITuple#getSourceIndex()} for each visited
     * index entry.
     */
    final public int getSourceIndex(int index) {
        
        if (sources.length == 1) {

            // One source - the index is always zero.
            
            return 0;

        }

        return sourceIndices[index];
        
    }

    /**
     * Return the commitTime of the index view from which this result set was
     * read. This may be used to force a {@link ITx#UNISOLATED} or
     * {@link ITx#READ_COMMITTED} chunked iterator to produce a consistent view
     * by issuing continuation queries for the commitTime that was returned by
     * the initial {@link ResultSet}.
     * 
     * @return
     */
    final public long getCommitTime() {

        return commitTime;
        
    }
    
    /**
     * Return the ordered array of sources from which the iterator read and the
     * {@link ResultSet} was populated.
     * <p>
     * The values returned by {@link ITuple#getSourceIndex()} may be used to
     * identify the resource from which a given tuple was read. That information
     * is used to direct {@link ITuple#readBlock(long)} requests to the correct
     * resource on the {@link IDataService}.
     */
    final public IResourceMetadata[] getSources() {
        
        return sources;
        
    }

    /**
     * The value of the <i>limit</i> specified to the ctor. This is the
     * dimension of the internal arrays used to buffer the data for the tuples.
     * <p>
     * Note: This field is transient - it does not get (de-)serialized.
     */
    protected int getLimit() {

        return limit;
        
    }
    transient private int limit;
    
    /**
     * The {@link IndexMetadata}.
     * <p>
     * Note: This field is transient - it does not get (de-)serialized.
     */
    transient private IndexMetadata indexMetadata;

    /**
     * Setup the internal buffers.
     * 
     * @param limit
     *            The maximum #of tuples that will be materialized. Use (-limit)
     *            for a soft limit. The caller should either use the suggested
     *            capacity, compute either an actual limit or the upper bound
     *            based on additional information, such as a range count, or
     *            treat the suggested capacity as a soft limit if it is not
     *            possible to determine the upper bound.
     */
    protected void init(int limit) {

        if (init)
            throw new IllegalStateException();

        init = true;
        
//        if (limit < 0)
//            throw new IllegalArgumentException();
        
        this.limit = limit;

        if (limit < 0) {

            // make the limit positive to allocate the arrays.
            limit = -limit;
            
        }
        
        this.keys = (sendKeys ? new RandomAccessByteArray(0,0,new byte[limit][]) : null);

        this.vals = (sendVals ? new RandomAccessByteArray(0,0,new byte[limit][]) : null);
        
        if(indexMetadata.getDeleteMarkers()) {
            
            // index has delete markers so we send them along.
            deleteMarkers = new byte[limit];
            
        }

        if(indexMetadata.getVersionTimestamps()) {

            // index has version timestamps so we send them along.
            versionTimestamps = new long[limit];

        }

        // if one source then the index is always zero
        // @todo byte[] presumes #sources in view <= 127, which it SHOULD be (not checked).
        sourceIndices = sources.length > 1 ? new byte[limit] : null;

    }
    private boolean init = false;
    
    /**
     * Increase the size of the internal buffers.
     */
    private void resize() {
        
        assert limit < 0;

        // double the limit.
        limit = limit * 2;
        
        int limit = this.limit;

        if (limit < 0)
            limit = -limit;

        // @todo restore info level once working smoothly.
//        if (log.isInfoEnabled()) {

            log.warn("resizing buffers: ntuples=" + ntuples + ", new limit=" + limit);

        // }

        if (this.keys != null) {

            this.keys = this.keys.resize(limit);

        }

        if (this.vals != null) {

            this.vals = this.vals.resize(limit);

        }

        if (this.deleteMarkers != null) {

            byte[] deleteMarkers = new byte[limit];
            
            System.arraycopy(this.deleteMarkers, 0, deleteMarkers, 0, ntuples);

            this.deleteMarkers = deleteMarkers;
            
        }

        if (this.versionTimestamps != null) {

            long[] versionTimestamps = new long[limit];

            System.arraycopy(this.versionTimestamps, 0, versionTimestamps, 0, ntuples);

            this.versionTimestamps = versionTimestamps;
            
        }

        if(this.sourceIndices != null) {

            byte[] sourceIndices = new byte[limit];
            
            System.arraycopy(this.sourceIndices, 0, sourceIndices, 0, ntuples);
            
            this.sourceIndices = sourceIndices;
            
        }
        
    }
    
    /**
     * Notify that the iterator is done and communicate metadata back to the
     * client about whether or not a continuation query should be issued against
     * this index partition.
     * <p>
     * Note: the point of this method is to communicate the restart point for a
     * continuation query (or that no continuation query is necessary). If you
     * are scanning ahead to decide whether or not the next coherent chunk of
     * tuples (e.g., a logical row) would fit in the buffer, then the restart
     * point is the point from which the continuation query should start and the
     * key to report is the last key before the start of the first logical row
     * that was rejected because it would overflow the internal buffers.
     * <p>
     * Note: <code>!exhausted</code> the caller will issue a continuation
     * query against this index partition whose <i>fromKey</i> is the successor
     * of <i>lastKey</i> (the successor is formed using
     * {@link BytesUtil#successor(byte[])}).
     * <p>
     * Note: If an index partition is <i>exhausted</i> there may still be data
     * for the key range on subsequent index partitions the caller will discover
     * the locator for the next index partition in key order whose left
     * separator key is LE the <i>toKey</i> and query it for more results. If
     * there is no such index partition then the aggregate iterator is finished.
     * 
     * @param exhausted
     *            <code>true</code> iff the source iterator will not visit any
     *            more tuples {@link Iterator#hasNext()} is <code>false</code>.
     * @param lastKey
     *            The key from the last tuple scanned by the source iterator
     *            regardless of whether it was included in the result set -or-
     *            <code>null</code> iff no tuples were scanned (this implies
     *            that there is no data for the query in the key range on this
     *            index partition and hence that it is <i>exhausted</i>).
     */
    protected void done(boolean exhausted, byte[] lastKey) {

        if (!exhausted && lastKey == null) {

            /*
             * The iterator must be exhausted if [lastKey == null] since that
             * implies that NO tuples were visited and hence that none lie
             * within the optional key-range constraint for this index
             * partition.
             */
            
            throw new IllegalArgumentException();
            
        }
        
        if (done)
            throw new IllegalStateException();
        
        done = true;
        
        this.exhausted = exhausted;
        
        this.lastKey = lastKey;
        
        if(log.isInfoEnabled())
        log.info("ntuples=" + ntuples // + ", capacity=" + capacity
                + ", exhausted=" + exhausted + ", sendKeys=" + sendKeys
                + ", sendVals=" + sendVals + ", deleteMarkers="
                + (deleteMarkers != null ? true : false) + ", timestamps="
                + (versionTimestamps != null ? true : false) + ", commitTime="
                + commitTime);

    }
    private boolean done = false;

    /**
     * <code>true</code> iff the internal buffers are full. 
     */
    protected boolean isFull() {

        return ntuples >= limit;
        
    }
    
    /**
     * Copies the data from the tuple into the internal buffers.
     * 
     * @param tuple
     *            The tuple.
     */
    protected void copyTuple(ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();
        
        assertRunning();

        if(isFull() && limit<0) {
            
            resize();
            
        }
        
        if (sendKeys) // @todo define and use copyKey(ITuple)?
            keys.add(tuple.getKey());

        if (sendVals) // @todo define and use copyValue(ITuple)?
            vals.add(tuple.getValue());

        if (deleteMarkers != null) {

            deleteMarkers[ntuples] = (byte) (tuple.isDeletedVersion() ? 1
                    : 0);

        }

        if (versionTimestamps != null) {

            versionTimestamps[ntuples] = tuple.getVersionTimestamp();

        }

        if (sourceIndices != null) {
            
            final int sourceIndex = tuple.getSourceIndex();
            
            assert sourceIndex < Byte.MAX_VALUE;
            
            sourceIndices[ntuples] = (byte) sourceIndex;
            
        }
        
        // #of results that will be returned.
        ntuples++;

    }
    
    /**
     * true once {@link #init(int)} has been called and until
     * {@link #done(byte[])} is called.
     */
    protected void assertRunning() {

        if (init && !done)
            return;
        
        throw new IllegalStateException();
        
    }
    
    protected static short VERSION0 = 0x0;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = ShortPacker.unpackShort(in);

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

//        rangeCount = (int) LongPacker.unpackLong(in);

        ntuples = (int) LongPacker.unpackLong(in);

        commitTime = in.readLong();
        
        final int nsources = (int) LongPacker.unpackLong(in);

        sources = new IResourceMetadata[nsources];
        
        exhausted = in.readBoolean();

        final boolean haveKeys = in.readBoolean(); sendKeys = haveKeys;

        final boolean haveVals = in.readBoolean(); sendVals = haveVals;

        final boolean haveDeleteMarkers = in.readBoolean();

        final boolean haveVersionTimestamps = in.readBoolean();

        final int lastKeySize = (int) LongPacker.unpackLong(in);
        if (lastKeySize != 0) {
            lastKey = new byte[lastKeySize];
            in.readFully(lastKey);
        } else {
            lastKey = null;
        }

        for (int i = 0; i < sources.length; i++) {
                
            sources[i] = (IResourceMetadata) in.readObject();

        }
        
        leafKeySerializer = (IDataSerializer)in.readObject();

        leafValSerializer = (IDataSerializer)in.readObject();
        
        tupleSerializer = (ITupleSerializer)in.readObject();
        
// if (ntuples == 0) {
//
//            // Nothing more to read.
//            
//            return;
//            
//        }
        
        if (haveKeys) {
            keys = new RandomAccessByteArray( 0, 0, new byte[ntuples][] );
            leafKeySerializer.read(in, keys);
        } else {
            keys = null;
        }

        if (haveVals) {
            vals = new RandomAccessByteArray(0, 0, new byte[ntuples][]);
            leafValSerializer.read(in, vals);
        } else {
            vals = null;
        }
        
        if (haveDeleteMarkers) {
            
            deleteMarkers = new byte[ntuples];

            if (ntuples > 0) {

                InputBitStream ibs = new InputBitStream((InputStream) in, 0/* unbuffered! */);

                // @todo ibs.read() does not handle ntuples==0 gracefully.
                
                ibs.read(deleteMarkers, ntuples/* len */);

                // ibs.close();

            }
            
        }

        if (haveVersionTimestamps) {
            
            versionTimestamps = new long[ntuples];
            
            for(int i=0; i<ntuples; i++) {
                
                versionTimestamps[i] = in.readLong();
                
            }
            
        }

        if (sources.length > 1) {
            
            sourceIndices = new byte[ntuples];
            
            // @todo use a compressed encoding, e.g., bit length or huffman.
            for(int i=0; i<ntuples; i++) {
                
                sourceIndices[i] = in.readByte();
                
            }
            
        } else {
            
            // The source index is always zero if there is just one source.
            sourceIndices = null;
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ShortPacker.packShort(out, VERSION0);

//        LongPacker.packLong(out, rangeCount);

        LongPacker.packLong(out, ntuples);

        out.writeLong(commitTime);
        
        LongPacker.packLong(out, sources.length);

        // @todo write as bits?
        out.writeBoolean(exhausted);

        out.writeBoolean(keys != null);

        out.writeBoolean(vals != null);

        out.writeBoolean(deleteMarkers != null);
        
        out.writeBoolean(versionTimestamps != null);
        
        LongPacker.packLong(out, lastKey == null ? 0 : lastKey.length);

        if (lastKey != null) {

            out.write(lastKey);

        }

        for (int i = 0; i < sources.length; i++) {

            out.writeObject(sources[i]);

        }
        
        out.writeObject(leafKeySerializer);

        out.writeObject(leafValSerializer);

        out.writeObject(tupleSerializer);
        
// if (ntuples == 0) {
//
//            // Nothing more to write.
//            
//            return;
//            
//        }
            
        if (keys != null) {

            leafKeySerializer.write(out, keys);
            
        }

        if (vals != null) {

            leafValSerializer.write(out, vals);

        }
        
        if (deleteMarkers != null && ntuples > 0) {
            
            OutputBitStream obs = new OutputBitStream((OutputStream) out, 0/* unbuffered! */);
            
            obs.write(deleteMarkers, ntuples/*len*/);
            
            obs.flush();
            
        }
        
        /*
         * @todo reuse the timestamp serialization logic from the NodeSerializer
         * once something efficient has been identifier, e.g., huffman encoding
         * of timestamps.
         * 
         * @todo config on IndexMetadata along with serialization for the
         * deleteMarkers.
         */

        if (versionTimestamps != null) {
            
            for (int i = 0; i < ntuples; i++) {

                out.writeLong(versionTimestamps[i]);
                
            }
            
        }

        if (sources.length > 1) {

            /*
             * Note: if only one source then we do not need the source indices
             * since the source index is always zero.
             */
            
            for(int i=0; i<ntuples; i++) {
                
                out.writeByte(sourceIndices[i]);
                
            }
            
        }
        
    }

    /**
     * Deserialization constructor.
     */
    public ResultSet() {
    }

    /**
     * The basic approach is:
     * <ol>
     * 
     * <li>Create a new {@link ResultSet}</li>
     * 
     * <li>Invoke {@link #init(int)} to setup the internal buffers.</li>
     * 
     * <li>Apply the source {@link ITupleIterator}, using
     * {@link #copyTuple(ITuple)} to copy data into those buffers.</li>
     * 
     * <li>Signal completion using {@link #done(boolean, byte[])}</li>
     * 
     * </ol>
     * 
     * @param ndx
     *            The index.
     * @param flags
     *            The flags specified for the iterator. See {@link IRangeQuery}.
     */
    public ResultSet(IIndex ndx, int flags) {

        if (ndx == null)
            throw new IllegalArgumentException();
        
        sendKeys = (flags & IRangeQuery.KEYS) != 0;

        sendVals = (flags & IRangeQuery.VALS) != 0;

        indexMetadata = ndx.getIndexMetadata();
        
        leafKeySerializer = indexMetadata.getLeafKeySerializer();
        
        leafValSerializer = indexMetadata.getLeafValueSerializer();
        
        tupleSerializer = indexMetadata.getTupleSerializer();
        
        sources = ndx.getResourceMetadata();

        commitTime = (ndx instanceof AbstractBTree ? ((AbstractBTree) ndx)
                .getLastCommitTime() : ((FusedView) ndx).srcs[0]
                .getLastCommitTime());

    }

    /**
     * Constructor used to populate the {@link ResultSet} directly from an
     * iterator.
     * <p>
     * Note: The <i>itr</i> provided to this method MUST be created with
     * {@link IRangeQuery#KEYS} so that we can report the lastKey visited for
     * continuation queries.
     * 
     * @param ndx
     *            The index.
     * @param capacity
     *            The requested capacity for the operation.
     * @param flags
     *            The flags specified for the iterator.
     * @param itr
     *            The source iterator.
     */
    public ResultSet(IIndex ndx, int capacity, int flags, ITupleIterator itr) {

        this(ndx, flags);
        
        // initialize the buffers.
        init(capacity);
        
        /*
         * Copy tuples into the result set buffers.
         */

        ITuple tuple = null;

        while (!isFull() && itr.hasNext()) {

            copyTuple(tuple = itr.next());
            
        }

        /*
         * True iff the source iterator will not visit any more tuples.
         */
        final boolean exhausted = !itr.hasNext();
        
        /*
         * We always return the lastKey visited and set lastKey := null iff no
         * keys were scanned. This ensures that tuples that are filtered out are
         * not re-scanned by a continuation query.
         */
        final byte[] lastKey = (tuple == null ? null : tuple.getKey());

        // Signal completion.
        done(exhausted, lastKey);

    }
    
}
