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
 * Created on Nov 5, 2006
 */

package com.bigdata.util;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;

import com.bigdata.io.IByteArraySlice;

/**
 * Utility class for computing the {@link Adler32} checksum of a buffer.  This
 * class is NOT thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This could stand some review in its API and its usage.
 */
public class ChecksumUtility {
	
    /**
     * ThreadLocal {@link ChecksumUtility} factory.
     */
	public static final ThreadLocal<ChecksumUtility> threadChk = new ThreadLocal<ChecksumUtility>() {
	    
	    protected ChecksumUtility initialValue() {
	        
	        return new ChecksumUtility();
	        
	    }
	    
	};
	
	/**
	 * static access to a ThreadLocal Checksum utility
	 * 
	 * @return the ChecksumUtility
	 * 
	 * @deprecated Use {@link #threadChk} and {@link ThreadLocal#get()}.
	 */
	public static ChecksumUtility getCHK() {
		ChecksumUtility chk = (ChecksumUtility) threadChk.get();
		
		if (chk == null) {
			chk = new ChecksumUtility();
			threadChk.set(chk);
		}
		
		return chk;
	}

    /**
     * Used to compute the checksums. Exposed to subclasses so they can update
     * the checksum for additional fields.
     */
    protected final Adler32 chk = new Adler32();

    /**
     * Compute the {@link Adler32} checksum of the buffer. The position, mark,
     * and limit are unchanged by this operation. The operation is optimized
     * when the buffer is backed by an array.
     * 
     * @param buf
     *            The buffer.
     *            
     * @return The checksum.
     */
    public int checksum(final ByteBuffer buf) {

        return checksum(buf, buf.position(), buf.limit());

    }
    
    /**
     * Compute the {@link Adler32} checksum of the buffer.  The position,
     * mark, and limit are unchanged by this operation.  The operation is
     * optimized when the buffer is backed by an array.
     * 
     * @param buf
     *            The buffer.
     * @param pos
     *            The starting position.
     * @param limit
     *            The limit.
     * 
     * @return The checksum.
     * 
     * @todo why pass in the pos/lim when [buf] could incorporate them?
     */
    public int checksum(final ByteBuffer buf, final int pos, final int limit) {
        
        assert buf != null;
        assert pos >= 0;
        assert limit > pos;

        // reset before computing the checksum.
        chk.reset();
    
        // update the checksum.
        update(buf, pos, limit);
        
        // The Adler checksum is a 32-bit value.
        return (int) chk.getValue();
        
    }

    /*
     * protected update methods.
     */
    
    /**
     * Reset the checksum.
     */
    protected void reset() {

        chk.reset();
        
    }

    /**
     * Return the Alder checksum, which is a 32bit value.
     */
    protected int getChecksum() {
        
        return (int) chk.getValue();

    }

    /**
     * Updates the {@link Adler32} checksum from the data in the buffer. The
     * position, mark, and limit are unchanged by this operation. The operation
     * is optimized when the buffer is backed by an array.
     * 
     * @param buf
     */
    protected void update(final ByteBuffer buf) {

        update(buf, buf.position(), buf.limit());
        
    }

    /**
     * Core implementation updates the {@link Adler32} checksum from the data in
     * the buffer. The position, mark, and limit are unchanged by this
     * operation. The operation is optimized when the buffer is backed by an
     * array.
     * 
     * @param buf
     * @param pos
     * @param limit
     */
    protected void update(final ByteBuffer buf, final int pos, final int limit) {

        assert buf != null;
        assert pos >= 0;
        assert limit > pos;

        // reset before computing the checksum.
//        chk.reset();

        if (buf.hasArray()) {

            /*
             * Optimized when the buffer is backed by an array.
             */
            
            final byte[] bytes = buf.array();
            
            final int len = limit - pos;
            
            if (pos > bytes.length - len) {
                
                throw new BufferUnderflowException();
            
            }
                
            chk.update(bytes, pos + buf.arrayOffset(), len);
            
        } else {

            /*
             * Compute the checksum of a byte[] on the native heap.
             * 
             * @todo this should be a JNI call. The Adler32 class is already a
             * JNI implementation.
             */
            
            if (a == null) {

                a = new byte[512];
                
            }

            // isolate changes to (pos,limit).
            final ByteBuffer b = buf.asReadOnlyBuffer();

            // stopping point.
            final int m = limit;
            
            // update checksum a chunk at a time.
            for (int p = pos; p < m; p += a.length) {

                // #of bytes to copy into the local byte[].
                final int len = Math.min(m - p, a.length);

                // set the view
                b.limit(p + len);
                b.position(p);
                
                // copy into Java heap byte[], advancing b.position().
                b.get(a, 0/* off */, len);

                // update the running checksum.
                chk.update(a, 0/* off */, len);

            }

// This is WAY to expensive since it is a JNI call per byte.
//            
//            for (int i = pos; i < limit; i++) {
//                
//                chk.update(buf.get(i));
//                
//            }
            
        }
        
    }
    
    /**
     * Private buffer used to incrementally compute the checksum of the data
     * as it is received. The purpose of this buffer is to take advantage of
     * more efficient bulk copy operations from the NIO buffer into a local
     * byte[] on the Java heap against which we then track the evolving
     * checksum of the data.
     */
    private volatile byte[] a;

    public int checksum(final IByteArraySlice slice) {
        
        assert slice != null;

        // reset before computing the checksum.
        chk.reset();
    
        chk.update(slice.array(), slice.off(), slice.len());
            
        /*
         * The Adler checksum is a 32-bit value.
         */
        
        return (int) chk.getValue();
        
    }

    public int checksum(final byte[] buf) {

        return checksum(buf, 0, buf.length);
        
    }

    public int checksum(final byte[] buf, int sze) {
        
        return checksum(buf, 0, sze);
        
    }
    
    public int checksum(final byte[] buf, int off, int sze) {
        
        assert buf != null;

        // reset before computing the checksum.
        chk.reset();
    
        chk.update(buf, off, sze);
            
        /*
         * The Adler checksum is a 32-bit value.
         */
        
        return (int) chk.getValue();
    
    }
    
}
