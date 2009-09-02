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
/*
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.data;

import java.io.Serializable;

import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Interface for coding (compressing) an {@link INodeData} or {@link ILeafData}
 * onto a byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAbstractNodeDataCoder<T extends IAbstractNodeData> extends
        Serializable {
    
    /**
     * Return <code>true</code> if this implementation can code data records for
     * B+Tree nodes.
     * 
     * @see INodeData
     */
    boolean isNodeDataCoder();

    /**
     * Return <code>true</code> if this implementation can code data records
     * for B+Tree leaves.
     * 
     * @see ILeafData
     */
    boolean isLeafDataCoder();

    /**
     * Encode the data.
     * <p>
     * Note: Implementations of this method are typically heavy. While it is
     * always valid to {@link #encode(IAbstractNodeData, DataOutputBuffer)} an
     * {@link IAbstractNodeData}, DO NOT invoke this <em>arbitrarily</em> on
     * data which may already be coded. The {@link IAbstractNodeCodedData}
     * interface will always be implemented for coded data.
     * 
     * @param node
     *            The node or leaf data.
     * @param buf
     *            A buffer on which the coded data will be written.
     * 
     * @return A slice onto the post-condition state of the caller's buffer
     *         whose view corresponds to the coded record. This may be written
     *         directly onto an output stream or the slice may be converted to
     *         an exact fit byte[].
     * 
     * @throws UnsupportedOperationException
     *             if {@link IAbstractNodeData#isLeaf()} is <code>true</code>
     *             and this {@link IAbstractNodeDataCoder} can not code B+Tree
     *             {@link ILeafData} records.
     * 
     * @throws UnsupportedOperationException
     *             if {@link IAbstractNodeData#isLeaf()} is <code>false</code>
     *             and this {@link IAbstractNodeDataCoder} can not code B+Tree
     *             {@link INodeData} records.
     */
    AbstractFixedByteArrayBuffer encode(T node, DataOutputBuffer buf);

    /**
     * Return an {@link IAbstractNodeData} instance which can access the coded
     * data.
     * 
     * @param data
     *            The record containing the coded data.
     * 
     * @return A view of the coded data.
     */
    T decode(AbstractFixedByteArrayBuffer data);

}
