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
 * Created on Dec 21, 2007
 */

package com.bigdata.io;

import java.nio.ByteBuffer;

import com.bigdata.btree.BytesUtil;

/**
 * An interface for absolute get/put operations on a fixed slice of a byte[].
 * <p>
 * Note: For efficiency reasons this interface deliberately DOES NOT support
 * protecting the backing byte[] against mutation.
 * <p>
 * Note: This interface is designed for uses where {@link ByteBuffer} is not
 * appropriate, including those which require access to the backing byte[] for
 * efficiency. The use of this interface is encourage over {@link ByteBuffer}
 * unless you are going to read or write directly on the file system or network
 * channels. If you are performing operations on in-memory data records then
 * implementations of this interface can allow much more efficient operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add bitwise operations with a given byte offset ala
 *       {@link BytesUtil#setBit(ByteBuffer, int, int, boolean)}. Copy the test
 *       suite as well.
 */
public interface IFixedByteArrayBuffer extends IRawRecord {

    /**
     * Return a copy of the data in the slice.
     * 
     * @return A new array containing data in the slice.
     * 
     * @see #asByteBuffer()
     */
    byte[] toByteArray();

    /**
     * Wraps the data in the slice within a {@link ByteBuffer} (does NOT copy
     * the data).
     * 
     * @return A {@link ByteBuffer} encapsulating a reference to the data in the
     *         slice.
     */
    ByteBuffer asByteBuffer();

}
