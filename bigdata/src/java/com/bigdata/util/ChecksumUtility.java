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
 */
public class ChecksumUtility {
	
	private static ThreadLocal threadChk = new ThreadLocal();
	/**
	 * static access to a ThreadLocal Checksum utility
	 * 
	 * @return the ChecksumUtility
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
     * Private helper object.
     */
    private final Adler32 chk = new Adler32();

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
     */
    public int checksum(final ByteBuffer buf, final int pos, final int limit) {
        
        assert buf != null;
        assert pos >= 0;
        assert limit > pos;

        // reset before computing the checksum.
        chk.reset();
        
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
            
            for (int i = pos; i < limit; i++) {
                
                chk.update(buf.get(i));
                
            }
            
        }
        
        /*
         * The Adler checksum is a 32-bit value.
         */
        
        return (int) chk.getValue();
        
    }

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

    public int checksum(final byte[] buf, int sze) {
        
        assert buf != null;

        // reset before computing the checksum.
        chk.reset();
    
        chk.update(buf, 0, sze);
            
        /*
         * The Adler checksum is a 32-bit value.
         */
        
        return (int) chk.getValue();
    }
}
