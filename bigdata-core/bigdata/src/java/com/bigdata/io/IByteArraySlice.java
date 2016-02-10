/*

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
 * Created on Aug 27, 2009
 */

package com.bigdata.io;

import com.bigdata.btree.keys.KeyBuilder;

/**
 * Interface for a slice of a backing byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IByteArraySlice {

    /**
     * The backing byte[]. This method DOES NOT guarantee that the backing array
     * reference will remain constant. Some implementations use an extensible
     * backing byte[] and will replace the reference when the backing buffer is
     * extended.
     */
    public byte[] array();

    /**
     * The start of the slice in the {@link #array()}.
     */
    public int off();

    /**
     * The length of the slice in the {@link #array()}.
     * 
     * Note: {@link IByteArraySlice#len()} has different semantics for some
     * concrete implementations. {@link ByteArrayBuffer#len()} always returns
     * the capacity of the backing byte[] while {@link ByteArrayBuffer#pos()}
     * returns the #of bytes written onto the backing buffer. In contrast,
     * {@link KeyBuilder#len()} is always the #of bytes written onto the backing
     * buffer.
     */
    public int len();

    /**
     * Return a copy of the data in the slice.
     * 
     * @return A new array containing data in the slice.
     */
    byte[] toByteArray();
    
}
