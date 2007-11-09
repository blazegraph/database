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
 * Created on Feb 12, 2007
 */

package com.bigdata.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IStoreSerializer;

/**
 * Helper utilities for (de-)serialization of {@link Serializable} objects using
 * the Java serialization mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write test suite.
 * 
 * @deprecated by {@link IStoreSerializer} which allows the serialized object to
 *             access the {@link IRawStore} reference (actually, this is still
 *             useful for serialization of application data that does not
 *             include addresses into a store).
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
