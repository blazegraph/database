/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 12, 2006
 */

package com.bigdata.btree;

import java.util.Arrays;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IBlock;

/**
 * A key-value pair used to facilitate some iterator constructs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Tuple implements ITuple {

    final private AbstractBTree btree;
    
    final private int flags;
    
    final private boolean needKeys;
    
    final private boolean needVals;
    
    public int getSourceIndex() {
        
        return 0;
        
    }
    
    public int flags() {
        
        return flags;
        
    }
    
    public boolean getKeysRequested() {
        
        return needKeys;
        
    }
    
    public boolean getValuesRequested() {
        
        return needVals;
        
    }

    /**
     * Reused for each key that is materialized and <code>null</code> if keys
     * are not being requested. Among other things, the {@link #kbuf} access may
     * be used to completely avoid heap allocations when considering keys. The
     * data for the current key are simply copied from the leaf into the
     * {@link #kbuf} and the application can either examine the data in the
     * {@link #kbuf} or copy it into its own buffers.
     */
    final private DataOutputBuffer kbuf;
    
    final public ByteArrayBuffer getKeyBuffer() {
        
        if (!needKeys)
            throw new UnsupportedOperationException();
        
        return kbuf;
        
    }

    /**
     * Return a stream from which the key may be read.
     * 
     * @throws UnsupportedOperationException
     *             if the keys were not requested.
     */
    final public DataInputBuffer getKeyStream() {

        if (!needKeys)
            throw new UnsupportedOperationException();

        if (keyStream == null) {

            // setup the input stream.

            keyStream = new DataInputBuffer(kbuf.array(), 0/* offset */, kbuf
                    .limit()/* len */);

        } else {

            // reset the buffer.

            keyStream.setBuffer(kbuf.array(), 0, kbuf.limit());

        }

        return keyStream;

    }

    private DataInputBuffer keyStream = null;

    /**
     * Return a stream from which the value may be read.
     * 
     * @throws UnsupportedOperationException
     *             if the values were not requested.
     */
    final public DataInputBuffer getValueStream() {

        if (!needVals)
            throw new UnsupportedOperationException();

        if (isNull)
            throw new UnsupportedOperationException();

        if (versionDeleted)
            throw new UnsupportedOperationException();

        if (valStream == null) {

            // setup the input stream.

            valStream = new DataInputBuffer(vbuf.array(), 0/* offset */, vbuf
                    .limit()/* len */);

        } else {

            // reset the buffer.

            valStream.setBuffer(vbuf.array(), 0, vbuf.limit());

        }
        
        return valStream;
        
    }
    private DataInputBuffer valStream = null;
    
    /**
     * Reused for each value that is materialized and <code>null</code> if
     * values are not being requested.
     */
    final private DataOutputBuffer vbuf;

    /**
     * <code>true</code> iff the iterator is visiting an index entry with a
     * <code>null</code> value.
     */
    private boolean isNull;
    
    private long versionTimestamp = 0L;

    /**
     * Note: The default of <code>false</code> has the correct semantics if
     * the btree does not support isolation since isolation requires that both
     * deleted entries and version timestamps are enabled.
     */
    private boolean versionDeleted = false;
    
    /**
     * The #of entries that have been visited. This will be ZERO (0) until the
     * first entry has been visited, at which point it is incremented to ONE
     * (1), etc.
     */
    private int nvisited = 0;
    
    /**
     * The #of entries that have been visited so far and ZERO (0) until the
     * first entry has been visited.
     */
    public long getVisitCount() {
        
        return nvisited;
        
    }
    
    /** Disallowed. */
    private Tuple() {
        
        throw new UnsupportedOperationException();
        
    }

    public Tuple(AbstractBTree btree, int flags) {
        
        if (btree == null)
            throw new IllegalArgumentException();
        
        this.btree = btree;
        
        this.flags = flags;
        
        needKeys = (flags & IRangeQuery.KEYS) != 0;
        
        needVals = (flags & IRangeQuery.VALS) != 0;
        
        if(needKeys) {
            
            /*
             * Note: we are choosing a smallish initial capacity. We could
             * specify ZERO (0) to never over estimate, but the buffer will be
             * re-sized automatically. An initial non-zero value makes it more
             * likely that we will not have to re-allocate. The smallish initial
             * value means that we are not allocating or wasting that much space
             * if we in fact require less (or more).
             */

            kbuf = new DataOutputBuffer(128);
            
        } else {
            
            kbuf = null;
            
        }
        
        if(needVals) {

            vbuf = new DataOutputBuffer(128);
            
        } else {
            
            vbuf = null;
            
        }
        
    }

    public byte[] getKey() {

        if (!needKeys)
            throw new UnsupportedOperationException();

        return kbuf.toByteArray();

    }

    public boolean isNull() {
    
        return isNull;
        
    }
    
    public byte[] getValue() {

        if (!needVals)
            throw new UnsupportedOperationException();

        if (versionDeleted || isNull)
            return null;

        return vbuf.toByteArray();

    }

    public ByteArrayBuffer getValueBuffer() {

        if(versionDeleted)
            throw new UnsupportedOperationException();

        if (isNull)
            throw new UnsupportedOperationException();
        
        return vbuf;
        
    }

    public Object getObject() {

        if (versionDeleted) {

            // Visiting a deleted entry.
            return null;
            
        }

        /*
         * Note: The tuple serializer will see tuples whose value is null.
         */

        return btree.metadata.getTupleSerializer().deserialize(this);
        
    }
    
    /**
     * @todo Implement. In order to work the tuple needs to know the source
     *       store from which the index entry was read. In the context of a
     *       fused view this is the element of that view that supplied the tuple
     *       binding (blocks may be stored on journals or index segments). In
     *       the context of a scale-out index, the state must include enough
     *       information to locate the data service for the index view and then
     *       the source index within that view.
     *       <p>
     *       Consider the notion of leases for the read back from a data
     *       service. If you do not read on the block soon enough the lease is
     *       gone and you will get an error if you try to read on the block. The
     *       lease can be used to limit the time that an index partition is
     *       locked to a data service.
     */
    public IBlock readBlock(long addr) {

        throw new UnsupportedOperationException();
        
    }
    
    public long getVersionTimestamp() {

        return versionTimestamp;
        
    }

    public boolean isDeletedVersion() {

        return versionDeleted;
        
    }

    /**
     * Copy data and metadata for the index entry from the {@link Leaf} into the
     * {@link Tuple} and increment the counter of the #of visited entries.
     * 
     * @param index
     *            The index entry.
     * @param leaf
     *            The leaf.
     */
    public void copy(int index, ILeafData leaf) {
        
        nvisited++;
        
        /*
         * true iff delete markers are enabled and this entry is marked deleted.
         */
        versionDeleted = leaf.hasDeleteMarkers() && leaf.getDeleteMarker(index);
        
        /*
         * the version timestamp iff timestamps are enabled and otherwise 0L.
         */
        versionTimestamp = leaf.hasVersionTimestamps() ? leaf
                .getVersionTimestamp(index) : 0L;

        if (needKeys) {

            kbuf.reset();

            leaf.copyKey(index, kbuf);

        }

        if (needVals) {

            vbuf.reset();

            if (!versionDeleted) {
             
                isNull = leaf.isNull(index);
                
                if(!isNull) {

                    leaf.copyValue(index, vbuf);
                    
                }
                
            }

        }
        
    }

    public String toString() {
        
        return super.toString()+
        "{nvisited="+nvisited+
        (versionDeleted ? ", deleted" : "")+
        (versionTimestamp == 0L ? "" : ", timestamp="+ getVersionTimestamp())+
        ", key="+(getKeysRequested()?Arrays.toString(getKey()):"N/A")+
        ", val="+(getValuesRequested()?(isNull()?"null":Arrays.toString(getValue())):"N/A")+
        "}";
        
    }
    
}
