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
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IAddressSerializer;
import com.bigdata.btree.NodeSerializer;

/**
 * An interface that encapsulates behaviors for (de-)serialization of objects on
 * an {@link IRawStore}.
 * <p>
 * Note that the methods declared on this interface MUST construct
 * {@link ObjectOutputStream}s that implement {@link IStoreObjectOutputStream}
 * and {@link ObjectInputStream}s that implement
 * {@link IStoreObjectInputStream} thereby make the {@link IRawStore} reference
 * available during (de-)serialization. This constraint makes it possible to
 * utilize the {@link IAddressManager} for the store during (de-)serialization.
 * 
 * @todo this is not being used much (or perhaps at all) at this time and needs
 *       to be looked into again. The main use case is the child addresses for
 *       the nodes of a {@link BTree}. See {@link NodeSerializer} and
 *       {@link IAddressSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStoreSerializer {

    /**
     * Serialize an object using the Java serialization mechanisms.
     * 
     * @param obj
     *            A {@link Serializable} object.
     * 
     * @return The serialized object state.
     */
    public byte[] serialize(Object obj);
    
    /**
     * De-serialize an object using the Java serialization mechanisms.
     * 
     * @param b
     *            A byte[] containing a serialized object.
     *            
     * @return The de-serialized object.
     */
    public Object deserialize(byte[] b);

    /**
     * De-serialize an object using the Java serialization mechanisms.
     * 
     * @param b
     *            A byte[] containing a serialized object.
     * @param off
     *            The offset of the first byte to de-serialize.
     * @param len
     *            The #of bytes in the object record.
     * 
     * @return The de-serialized object.
     */
    public Object deserialize(byte[] b, int off, int len);

    /**
     * De-serialize an object using the Java serialization mechanisms.
     * 
     * @param buf
     *            A buffer containing a serialized object. The bytes from
     *            {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}
     *            will be de-serialized and the position will be advanced to the
     *            limit.
     * 
     * @return The de-serialized object.
     */
    public Object deserialize(ByteBuffer buf);
    
}
