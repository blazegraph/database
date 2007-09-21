/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * An interface that encapsulates behaviors for (de-)serialization of objects on
 * an {@link IRawStore}.
 * <p>
 * Note that the methods declared on this interface MUST construct
 * {@link ObjectOutputStream}s that implement {@link IStoreObjectOutputStream} and
 * {@link ObjectInputStream}s that implement {@link IStoreObjectInputStream} thereby
 * make the {@link IRawStore} reference available during (de-)serialization.
 * This constraint makes it possible to utilize the {@link IAddressManager} for
 * the store during (de-)serialization.
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
     * De-serialize an object using the Java serialization mechansisms.
     * 
     * @param b
     *            A byte[] containing a serialized object.
     *            
     * @return The de-serialized object.
     */
    public Object deserialize(byte[] b);

    /**
     * De-serialize an object using the Java serialization mechansisms.
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
     * De-serialize an object using the Java serialization mechansisms.
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
