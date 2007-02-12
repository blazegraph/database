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
 * Created on Feb 12, 2007
 */

package com.bigdata.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Helper utilities for (de-)serialization of {@link Serializable} objects using
 * the Java serialization mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write test suite.
 */
public class SerializerUtil {

// This is not useful since we have to adjust the length of the byte[] anyway.
//    private static class BAOS extends  ByteArrayOutputStream {
//        
//        public BAOS(int size) {
//
//            super(size);
//            
//        }
//        
//        final public byte[] getBuffer() {
//            
//            return buf;
//            
//        }
//        
//    }

    /**
     * Serialize an object using the Java serialization mechansims.
     * 
     * @param obj A {@link Serializable} object.
     * 
     * @return The serialized object state.
     */
    static final public byte[] serialize(Object obj) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(obj);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return baos.toByteArray();

    }

    /**
     * De-serialize an object using the Java serialization mechansisms.
     * 
     * @param b
     *            A byte[] containing a serialized object.
     *            
     * @return The de-serialized object.
     */
    static final public Object deserialize(byte[] b) {

        return deserialize(b, 0, b.length);

    }

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
    static final public Object deserialize(byte[] b, int off, int len) {

        ByteArrayInputStream bais = new ByteArrayInputStream(b, off, len);

        try {

            ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

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
    static final public Object deserialize(ByteBuffer buf) {

        if (buf.hasArray()) {

            int off = buf.arrayOffset();

            int len = buf.remaining();

            Object ret = deserialize(buf.array(), off, len);
            
            buf.position(buf.limit());
            
            return ret;

        }

        ByteBufferInputStream bais = new ByteBufferInputStream(buf);

        try {

            ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

}
