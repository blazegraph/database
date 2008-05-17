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
 * Created on Dec 26, 2006
 */

package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;

/**
 * Interface for (de-)serialization of addresses of child nodes and leaves as
 * recorded on a {@link Node}.
 * <p>
 * Note: it is possible to use additional information from the
 * {@link IAddressManager} associated with an {@link IRawStore} to decide how to
 * (de-)serialize the addresses. This does have the effect of making the binary
 * format of the serialized addresses different from store to store, e.g., if
 * the #of offset bits is different on the two stores. However, we do NOT copy
 * index nodes using a binary format from one store to another. Instead,
 * overflow processing uses an {@link ITupleIterator} to visit the keys and
 * values and replicate them onto the new store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see NodeSerializer
 */
public interface IAddressSerializer extends Serializable {

    /**
     * De-serialize the child addresses for a node.
     * 
     * @param is
     *            The input stream.
     * @param childAddr
     *            The array into which the addresses must be written.
     * @param nchildren
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be read from the buffer and
     *            written on the array.
     */
    public void getChildAddresses(IAddressManager addressManager, DataInput is,
            long[] childAddr, int nchildren) throws IOException;

    /**
     * Serialize the child addresses for a node.
     * 
     * @param os
     *            The output stream.
     * @param childAddr
     *            The array of child addresses to be written.
     * @param nchildren
     *            The #of valid values in the array. The values in indices
     *            [0:n-1] are defined and must be written.
     */
    public void putChildAddresses(IAddressManager addressManager,
            DataOutputBuffer os, long[] childAddr, int nchildren)
            throws IOException;

}
