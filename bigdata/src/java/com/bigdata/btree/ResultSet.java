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

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;

/**
 * An object used to stream key scan results back to the client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResultSet implements Externalizable {

    protected static final Logger log = Logger.getLogger(ResultSet.class);

    /**
     * 
     */
    private static final long serialVersionUID = 8823205844134046434L;

    private int rangeCount;

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
     * Total #of key-value pairs within the key range (approximate).
     */
    public int getRangeCount() {
       
        return rangeCount;
        
    }

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
    public boolean isExhausted() {
        
        return exhausted;
        
    }

    /**
     * The last key visited by the iterator <em>regardless</em> of the
     * filter imposed -or- <code>null</code> iff no keys were visited by
     * the iterator for the specified key range.
     * 
     * @see #successor()
     */
    public byte[] getLastKey() {
        
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
    public byte[] successor() {

        if (lastKey == null)
            throw new UnsupportedOperationException();

        return BytesUtil.successor(lastKey);

    }

//    /**
//     * The visited keys iff the keys were requested.
//     */
//    public byte[][] getKeys() {
//        
//        return keys;
//        
//    }

    /**
     * Return the keys.
     * 
     * @throws IllegalStateException
     *             if the keys were not retrieved.
     */
    public IRandomAccessByteArray getKeys() {
        
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
    public IRandomAccessByteArray getValues() {
        
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
    public byte[] getKey(int index) {
        
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
    public byte[] getValue(int index) {
        
        if (vals == null)
            throw new IllegalStateException();
        
        return vals.getKey(index);
        
    }
    
//    /**
//     * The visited values iff the values were requested.
//     */
//    public byte[][] getValues() {
//        
//        return vals;
//        
//    }

    /**
     * The visited version timestamps iff the index maintains version
     * timestamps.
     * 
     * @return The version timestamps -or- <code>null</code> iff the index
     *         does not maintain version timestamps.
     */
    public long[] getVersionTimestamps() {
        
        return versionTimestamps;
        
    }
    
    /**
     * The visited delete markers iff the the index maintains delete markers.
     * The bytes are coded as ONE (1) is true and ZERO (0) is false.
     * 
     * @return The delete markers -or- <code>null</code> iff the index does
     *         not maintain delete markers.
     */
    public byte[] getDeleteMarkers() {
        
        return deleteMarkers;
        
    }

    /**
     * The values returned by {@link ITuple#getSourceIndex()} for each visited
     * index entry.
     */
    public int getSourceIndex(int index) {
        
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
    public long getCommitTime() {

        return commitTime;
        
    }
    
    /**
     * Deserialization constructor.
     */
    public ResultSet() {
    }

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
    
    /**
     * The {@link ITupleSerializer} that may be used to de-serialize the tuples
     * in the {@link ResultSet}.
     */
    public ITupleSerializer getTupleSerializer() {
        
        return tupleSerializer;
        
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
    public IResourceMetadata[] getSources() {
        
        return sources;
        
    }
    
    /**
     * Constructor used by the {@link DataService} to populate the
     * {@link ResultSet}.
     * 
     * @param ndx
     * @param fromKey
     * @param toKey
     * @param capacity
     * @param flags
     * @param filter
     */
    public ResultSet(IIndex ndx, byte[] fromKey, byte[] toKey, int capacity,
            int flags, ITupleFilter filter) {

        /*
         * The upper bound on the #of key-value pairs in the range.
         * 
         * Note: truncated to [int].
         */
        rangeCount = (int) ndx.rangeCount(fromKey, toKey);

        final int limit = (rangeCount > capacity ? capacity : rangeCount);

//        int ntuples = 0;

        final boolean sendKeys = (flags & IRangeQuery.KEYS) != 0;

        final boolean sendVals = (flags & IRangeQuery.VALS) != 0;

        keys = (sendKeys ? new RandomAccessByteArray(0,0,new byte[limit][]) : null);

        vals = (sendVals ? new RandomAccessByteArray(0,0,new byte[limit][]) : null);
        
        final IndexMetadata indexMetadata = ndx.getIndexMetadata();
        
        if(indexMetadata.getDeleteMarkers()) {
            
            // index has delete markers so we send them along.
            deleteMarkers = new byte[limit];
            
        }

        if(indexMetadata.getVersionTimestamps()) {

            // index has version timestamps so we send them along.
            versionTimestamps = new long[limit];

        }

        leafKeySerializer = indexMetadata.getLeafKeySerializer();
        
        leafValSerializer = indexMetadata.getLeafValueSerializer();
        
        tupleSerializer = indexMetadata.getTupleSerializer();
        
        sources = ndx.getResourceMetadata();

        // if one source then the index is always zero @todo presumes #sources in view <= 127
        sourceIndices = sources.length > 1 ? new byte[limit] : null;

        commitTime = (ndx instanceof AbstractBTree ? ((AbstractBTree) ndx)
                .getLastCommitTime() : ((FusedView) ndx).srcs[0]
                .getLastCommitTime());
        
        /*
         * Iterator that will visit the key range.
         * 
         * Note: We always visit the keys regardless of whether we pass them on
         * to the caller. This is necessary in order for us to set the [lastKey]
         * field on the result set.
         */
        final ITupleIterator itr = ndx.rangeIterator(fromKey, toKey, capacity,
                flags | IRangeQuery.KEYS, filter);

        /*
         * true if any keys were visited regardless of whether or not they
         * satisified the optional filter. This is used to make sure that we
         * always return the lastKey visited if any keys were visited and
         * otherwise set lastKey := null.
         */
        boolean anything = false;

        /*
         * @todo we could directly serialize the keys and values into a compact
         * data model as they are copied out of the tuple.
         */
        ITuple tuple = null;
        
        if ((flags & IRangeQuery.REMOVEALL) != 0) {

            log.info("Iterator will remove up to " + capacity
                    + " entries (capacity=" + capacity + ", rangeCount="
                    + rangeCount + ")");
            
        }
        
        while (ntuples < limit && itr.hasNext()) {

            anything = true;

            tuple = itr.next();
            
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

//        this.ntuples = ntuples;

        this.lastKey = (anything ? tuple.getKey() : null);

        this.exhausted = !itr.hasNext();

        log.info("ntuples=" + ntuples + ", capacity=" + capacity
                + ", exhausted=" + exhausted + ", sendKeys=" + sendKeys
                + ", sendVals=" + sendVals + ", deleteMarkers="
                + (deleteMarkers != null ? true : false) + ", timestamps="
                + (versionTimestamps != null ? true : false) + ", commitTime="
                + commitTime);

    }

    protected static short VERSION0 = 0x0;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = ShortPacker.unpackShort(in);

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        rangeCount = (int) LongPacker.unpackLong(in);

        ntuples = (int) LongPacker.unpackLong(in);

        commitTime = in.readLong();
        
        final int nsources = (int) LongPacker.unpackLong(in);

        sources = new IResourceMetadata[nsources];
        
        exhausted = in.readBoolean();

        final boolean haveKeys = in.readBoolean();

        final boolean haveVals = in.readBoolean();

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

        LongPacker.packLong(out, rangeCount);

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
         * of timestamps.  @todo config on IndexMetadata along with serialization
         * for the deletemarkers.
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

}
