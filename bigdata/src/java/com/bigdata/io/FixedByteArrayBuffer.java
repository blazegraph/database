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
/*
 * Created on Dec 23, 2007
 */

package com.bigdata.io;

import com.bigdata.btree.BytesUtil;

/**
 * Efficient absolute get/put operations on a slice of a byte[]. This class is
 * not thread-safe under mutation because the operations are not atomic.
 * Concurrent operations on the same region of the slice can reveal partial
 * updates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FixedByteArrayBuffer extends AbstractFixedByteArrayBuffer {

    /**
     * An empty slice.
     */
    public static final transient FixedByteArrayBuffer EMPTY = new FixedByteArrayBuffer(
            BytesUtil.EMPTY, 0, 0);
    
    /**
     * The backing byte[].
     */
    private final byte[] buf;

    final public byte[] array() {

        return buf;

    }

    /**
     * Create an instance backed by a fixed capacity byte[].
     * 
     * @param capacity
     *            The capacity of the backing byte[].
     */
    public FixedByteArrayBuffer(final int capacity) {

        super(0, capacity);

        this.buf = new byte[capacity];
        
    }

    /**
     * Create a slice of a byte[].
     * 
     * @param buf
     *            The byte[].
     * @param off
     *            The starting offset of the slice.
     * @param len
     *            The length of the slice.
     */
    public FixedByteArrayBuffer(final byte[] buf, final int off, final int len) {

        super(off, len);

        if (buf == null)
            throw new IllegalArgumentException("buf");

        if (off + len > buf.length)
            throw new IllegalArgumentException("off+len>buf.length");

        this.buf = buf;

    }

}
