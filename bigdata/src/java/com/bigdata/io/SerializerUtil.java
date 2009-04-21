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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Helper utilities for (de-)serialization of {@link Serializable} objects using
 * the Java serialization mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SerializerUtil {

    /**
     * An {@link IStreamSerializer} that uses java default serialization.
     */
    public static final IStreamSerializer STREAMS = new IStreamSerializer() {

        /**
         * 
         */
        private static final long serialVersionUID = -2625456835905636703L;

        public Object deserialize(ObjectInput in) {

            try {

                return in.readObject();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }

        public void serialize(ObjectOutput out, Object obj) {

            try {

                out.writeObject(obj);

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

        /**
         * Always returns the same reference.
         */
        private Object readResolve() throws ObjectStreamException {

            return STREAMS;

        }

        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {

            // NOP

        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {

            // NOP

        }
    };
    
    /**
     * An {@link IRecordSerializer} wrapper for the static methods declared by
     * the {@link SerializerUtil}.
     */
    public static final IRecordSerializer RECORDS = new IRecordSerializer() {

        /**
         * 
         */
        private static final long serialVersionUID = -2625456835905636703L;

        public Object deserialize(byte[] data) {
            
            return SerializerUtil.deserialize(data);
            
        }

        public byte[] serialize(Object obj) {
            
            return SerializerUtil.serialize(obj);
            
        }
        
        /**
         * Always returns the same reference.
         */
        private Object readResolve() throws ObjectStreamException {
            
            return RECORDS;
            
        }
        
        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {

            // NOP
            
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {

            // NOP

        }
    
    };
    
    /**
     * Serialize an object using the Java serialization mechansims.
     * 
     * @param obj
     *            A {@link Serializable} object.
     * 
     * @return The serialized object state.
     */
    static final public byte[] serialize(Object obj) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(obj);

            oos.flush();

            oos.close();
            
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
     * @return The de-serialized object -or- <code>null</code> iff the byte[]
     *         reference is <code>null</code>.
     */
    static final public Object deserialize(byte[] b) {

        if (b == null)
            return null;
        
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
    static final public Object deserialize(final byte[] b, final int off, final int len) {

        final ByteArrayInputStream bais = new ByteArrayInputStream(b, off, len);

        try {

            final ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();

        } catch (Exception ex) {

            /*
             * Note: an error here is often an attempt to deserialize an empty
             * byte[].
             */
            throw new RuntimeException("off=" + off + ", len=" + len, ex);

        }

    }

    /**
     * De-serialize an object using the Java serialization mechansisms.
     * 
     * @param is
     *            The input stream from which to read the serialized data.
     * 
     * @return The de-serialized object.
     */
    static final public Object deserialize(InputStream is) {

        try {

            final ObjectInputStream ois = new ObjectInputStream(is);

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
    static final public Object deserialize(final ByteBuffer buf) {

        if (true && buf.hasArray()) {

            final int off = buf.arrayOffset();

            final int len = buf.remaining();

            Object ret = deserialize(buf.array(), off, len);
            
            buf.position(buf.limit());
            
            return ret;

        }

        final ByteBufferInputStream bais = new ByteBufferInputStream(buf);

        try {

            final ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

}
