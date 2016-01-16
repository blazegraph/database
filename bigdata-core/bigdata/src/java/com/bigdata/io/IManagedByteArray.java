/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on May 25, 2011
 */

package com.bigdata.io;

/**
 * An interface for a managed byte[]. Implementations of this interface may
 * permit transparent extension of the managed byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IManagedByteArray extends IByteArraySlice {

    /**
     * Reset the buffer length to zero.
     * 
     * @return <i>this</i>.
     */
    IManagedByteArray reset();

    /**
     * Append a byte into the managed byte[]. The backing buffer will be
     * automatically extended if necessary.
     * 
     * @param b
     *            The byte.
     */
    IManagedByteArray append(byte b);
    
    /**
     * Append data into the managed byte[]. The backing buffer will be
     * automatically extended if necessary.
     * 
     * @param a
     *            The source data.
     */
    IManagedByteArray append(byte[] a);
    
    /**
     * Append data into the managed byte[]. The backing buffer will be
     * automatically extended if necessary.
     * 
     * @param a
     *            The source data.
     * @param off
     *            The offset of the first byte to be copied.
     * @param len
     *            The #of bytes to be copied.
     */
    IManagedByteArray append(byte[] a,int off, int len);

    /**
     * Return the capacity of the backing buffer.
     */
    int capacity();

    /**
     * Ensure that at least <i>len</i> bytes are free in the buffer. The buffer
     * may be grown by this operation but it will not be truncated.
     * <p>
     * This operation is equivalent to
     * 
     * <pre>
     * ensureCapacity(len() + len)
     * </pre>
     * 
     * and the latter is often used as an optimization.
     * 
     * @param len
     *            The minimum #of free bytes.
     */
    void ensureFree(int len);

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total bytes.
     * The buffer may be grown by this operation but it will not be truncated.
     * 
     * @param capacity
     *            The minimum #of bytes in the buffer.
     */
    void ensureCapacity(int capacity);
    
}
