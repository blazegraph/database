/*

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
package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.isolation.UnisolatedBTree;

/**
 * This procedure computes a range count on an index.
 */
public class RangeCountProcedure implements IIndexProcedure,
        IReadOnlyOperation, IParallelizableIndexProcedure, Externalizable {

    private static final long serialVersionUID = 5856712176446915328L;

    private byte[] fromKey;
    
    private byte[] toKey;

    /**
     * De-serialization ctor.
     *
     */
    public RangeCountProcedure() {
        
    }

    /**
     * Range count using the specified bounds.
     * 
     * @param fromKey
     *            The lower bound (inclusive) -or- <code>null</code> if there
     *            is no lower bound.
     * @param toKey
     *            The upper bound (exclusive) -or- <code>null</code> if there
     *            is no upper bound.
     */
    public RangeCountProcedure(byte[] fromKey, byte[] toKey) {

        this.fromKey = fromKey;
        
        this.toKey = toKey;
        
    }

    /**
     * <p>
     * Range count of entries in a key range for the index.
     * </p>
     * <p>
     * Note: This method reports the upper bound estimate of the #of key-value
     * pairs in the key range of the index. The cost of computing this estimate
     * is comparable to two index lookup probes. The estimate is an upper bound
     * because deleted entries in an {@link UnisolatedBTree} or a view thereof
     * that have not been eradicated through a suitable compacting merge will be
     * reported. An exact count may be obtained using a range iterator by NOT
     * requesting either the keys or the values.
     * </p>
     * 
     * @return The upper bound estimate of the #of key-value pairs in the key
     *         range of the named index.
     */
    public Object apply(IIndex ndx) {

        final long rangeCount = ndx.rangeCount(fromKey, toKey);
        
        return new Long( rangeCount );

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        
        final short version = in.readShort();

        if (version != VERSION0) {

            throw new IOException("Unknown version: " + version);
            
        }
     
        // fromKey
        {
            
            final int len = in.readInt();
            
            if(len > 0) {
                
                fromKey = new byte[len - 1];
                
                in.readFully(fromKey);
                
            }
            
        }

        // toKey
        {
            
            final int len = in.readInt();
            
            if(len > 0) {
                
                toKey = new byte[len - 1];
                
                in.readFully(toKey);
                
            }

        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeShort(VERSION0);

        /*
         * Note: 0 indicates a null reference. Otherwise the length of the
         * byte[] is written as (len + 1).
         */

        out.writeInt(fromKey == null ? 0 : fromKey.length + 1);

        if (fromKey != null) {

            out.write(fromKey);

        }

        out.writeInt(toKey == null ? 0 : toKey.length + 1);

        if (toKey != null) {

            out.write(toKey);

        }

    }

    private static final short VERSION0 = 0x0;

}
