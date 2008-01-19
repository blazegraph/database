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

import com.bigdata.io.ByteArrayBufferWithPosition;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.IByteArrayBuffer;

/**
 * A key-value pair used to facilitate some iterator constructs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Tuple implements ITuple {

    /**
     * True iff the keys for the visited entries are required by the application
     * consuming the iterator.
     */
    final /*public*/ boolean needKeys;
    
    /**
     * True iff the values for the visited entries are required by the application
     * consuming the iterator.
     */
    final /*public*/ boolean needVals;
    
    /**
     * True iff the keys for the visited entries were requested when the
     * {@link Tuple} was initialized.
     */
    public boolean getKeysRequested() {
        
        return needKeys;
        
    }
    
    /**
     * True iff the values for the visited entries were requested when the
     * {@link Tuple} was initialized.
     */
    public boolean getValuesRequested() {
        
        return needVals;
        
    }
    
    /**
     * Reused for each key that is materialized and <code>null</code> if keys
     * are not being requested. Among other things, the {@link #kbuf} access may
     * be used to completely avoid heap allocations when considering keys. The
     * data for the current key are simply copied from the leaf into the
     * {@link #kbuf} and the application can either examine the data in the
     * {@link #kbuf} or copy it into its own buffers. The key will always begin
     * at ZERO(0) within the #kbuf and extend up to (but exclusive of) the
     * current {@link DataOutputBuffer#position()} in the buffer. The
     * application MUST NOT modify the {@link DataOutputBuffer#mark()} as that
     * is used to efficiently copy keys that have been broken into a shared
     * prefix and a per-key remainder.
     */
    final /*public*/ ByteArrayBufferWithPosition kbuf;
    
    final public IByteArrayBuffer getKeyBuffer() {
        
        if(!needKeys) throw new UnsupportedOperationException();
        
        return kbuf;
        
    }
    
//    /**
//     * Reused for each value that is materialized and <code>null</code> if
//     * values are not being requested.
//     * 
//     * @todo this can only be used when the values are byte[]s. when they are
//     *       Java objects (other than a byte[]) we need to just store the
//     *       reference to the deserialized object.
//     */
//    final /*public*/ DataOutputBuffer vbuf;

    /**
     * Package private counter is updated by the iterator to reflect the #of
     * entries that have been visited. This will be ZERO (0) until the first
     * entry has been visited, at which point it is incremented to ONE (1), etc.
     * <p>
     * Note: This is package private since {@link IEntryIterator}s that produce
     * a fused view from multiple source {@link IEntryIterator}s are not able
     * to directly report the total #of visited enties using a field -- a method
     * call is required.
     */
    /*public*/ int nvisited = 0;
    
    /**
     * The #of entries that have been visited so far and ZERO (0) until the
     * first entry has been visited.
     */
    public long getVisitCount() {
        
        return nvisited;
        
    }
    
    public Tuple() {
        
        this(IRangeQuery.KEYS|IRangeQuery.VALS);
        
    }

    public Tuple(int flags) {
        
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
        
//        vbuf = null;
        
    }

    /**
     * Returns a copy of the current key.
     * <p>
     * Note: This causes a heap allocation. See {@link #kbuf} to avoid that
     * allocation.
     * 
     * @throws UnsupportedOperationException
     *             if keys are not being materialized.
     */
    public byte[] getKey() {
        
        if(!needKeys) throw new UnsupportedOperationException();

        return kbuf.toByteArray();
        
    }
    
//    public Object getValue() {
//        
//        if(!needVals) throw new UnsupportedOperationException();
//
//        return vbuf.toByteArray();
//        
//    }
    
    public Object val;
    
}
