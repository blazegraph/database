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
package com.bigdata.sparse;

import java.io.IOException;
import java.util.Date;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * A type safe enumeration of value types. This class also supports encoding
 * and decoding of instances of the enumerated value types and is used for
 * encoding and decoding of column values.
 */
public enum ValueType {

    /**
     * A 32-bit integer.
     */
    Integer(1),
    /**
     * A 64-bit integer.
     */
    Long(2),
    /**
     * A single precision floating point value.
     */
    Float(3),
    /**
     * A double precision floating point value.
     */
    Double(4),
    /**
     * A Unicode string.
     */
    Unicode(5),
    /**
     * A date is serialized as a long integer.
     */
    Date(6),
    /**
     * An uninterpreted byte array.
     */
    ByteArray(7)
    ;
    
    private final int code;

    ValueType(int code) {

        this.code = code;
        
    }

    /**
     * An integer that identifies the type safe enumeration value.
     * 
     * @return
     */
    public int intValue() {
        
        return code;
        
    }

    public static ValueType valueOf(int code) {
        
        switch(code) {
        case 1: return Integer;
        case 2: return Long;
        case 3: return Float;
        case 4: return Double;
        case 5: return Unicode;
        case 6: return Date;
        case 7: return ByteArray;
        default:
            throw new IllegalArgumentException("Unknown code: "+code);
        }
        
    }
    
    /**
     * The encoding used to serialize Unicode data.
     */
    private static final String UTF8 = "UTF-8";
    
    /**
     * Thread local buffer.
     */
    private static ThreadLocal buf = new ThreadLocal() {
        
        protected synchronized Object initialValue() {
        
            return new DataOutputBuffer();
            
        }
        
    };

    /**
     * Return a {@link ThreadLocal} buffer that is used to serialize values.
     */
    public static DataOutputBuffer getBuffer() {

        return (DataOutputBuffer)buf.get();
        
    }

    /**
     * Encode an object that is an instance of a supported class.
     * 
     * @param v
     *            The value.
     * 
     * @return The serialized byte[] encoding that value -or- null iff the
     *         value is null.
     * 
     * @exception UnsupportedOperationException
     *                if the value is not an instance of a supported class.
     */
    public static byte[] encode(Object v) {

        try {

            if (v == null) {

                /*
                 * A null will be interpreted as a deletion request for a
                 * column value on insert / update.
                 */
                return null;

            }

            DataOutputBuffer buf = getBuffer();
            
            buf.reset();

            if (v instanceof byte[]) {

                buf.writeByte(ValueType.ByteArray.intValue());

                // @todo constrain max byte[] length?
                byte[] bytes = (byte[])v;
                
                buf.packLong(bytes.length);
                
                buf.write(bytes);
                
            } else if (v instanceof Number) {

                if (v instanceof Integer) {

                    buf.writeByte(ValueType.Integer.intValue());

                    buf.writeInt(((Number) v).intValue());

                } else if (v instanceof Long) {

                    buf.writeByte(ValueType.Long.intValue());

                    buf.writeLong(((Number) v).longValue());
                    
                } else if (v instanceof Float) {

                    buf.writeByte(ValueType.Float.intValue());

                    buf.writeFloat(((Number) v).floatValue());
                    
                } else if (v instanceof Double) {

                    buf.writeByte(ValueType.Double.intValue());

                    buf.writeDouble(((Number) v).doubleValue());
                    
                } else {
                    
                    throw new UnsupportedOperationException();
                    
                }
                
            } else if (v instanceof Date) {

                buf.writeByte(ValueType.Date.intValue());

                buf.writeLong(((Date)v).getTime());
                
            } else if (v instanceof String) {

                buf.writeByte(ValueType.Unicode.intValue());
                
                // @todo constrain max byte[] length?
                byte[] bytes = ((String)v).getBytes(UTF8);
                
                buf.packLong( bytes.length );
                
                buf.write( bytes );
                
            } else {

                throw new UnsupportedOperationException();

            }
            
            return buf.toByteArray();
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }

    /**
     * 
     * @param v
     * @return
     */
    public static Object decode(byte[] v) {

        if (v == null)
            return null;

        if (v.length == 0)
            throw new IllegalArgumentException("Zero length byte[]");

        DataInputBuffer buf = new DataInputBuffer(v, 0, v.length);

        try {
            ValueType type = ValueType.valueOf((int) buf.readByte());

            switch (type) {

            case Integer:
                return buf.readInt();
            case Long:
                return buf.readLong();
            case Float:
                return buf.readFloat();
            case Double:
                return buf.readDouble();
            case Unicode: {
                int len = (int)buf.unpackLong();
                byte[] bytes = new byte[len];
                buf.readFully(bytes);
                String s = new String(bytes,UTF8);
                return s;
            }
            case Date:
                return new java.util.Date(buf.readLong());
            case ByteArray: {
                int len = (int)buf.unpackLong();
                byte[] bytes = new byte[len];
                buf.readFully(bytes);
                return bytes;
            }
            default:
                throw new AssertionError();

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
    }
    
}
