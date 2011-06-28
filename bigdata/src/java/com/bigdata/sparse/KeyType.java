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

import com.bigdata.rawstore.Bytes;

/**
 * A type safe enumeration of key types and the byte values that are used to
 * encode that key type within the encoded {@link Schema} name.
 * 
 * @see Schema#getSchemaBytes()
 */
public enum KeyType {

    Integer(0, Bytes.SIZEOF_INT),

    Long(1, Bytes.SIZEOF_LONG),

    Float(2, Bytes.SIZEOF_INT),

    Double(3, Bytes.SIZEOF_LONG),

    /** Variable length Unicode string. */
    Unicode(4, 0/* variable length */),

    /** Variable length ASCII string. */
    ASCII(5, 0/* variable length */),

    Date(6, Bytes.SIZEOF_LONG),
    
//    /** Variable length unsigned byte[] (no translation). */
//    UnsignedBytes(7, 0/* variable length */)
    ;

    private KeyType(int b, int encodedLength) {

        this.b = (byte) b;

        this.encodedLength = encodedLength;
        
    }

    /** The unique one byte code for this {@link KeyType}. */
    private final byte b;
    
    /**
     * The #of bytes in which values of that {@link KeyType} are encoded -or-
     * zero (0) iff values are encoded in a variable number of bytes with a
     * <code>nul</code> terminator for the byte sequence.
     */
    private final int encodedLength;
    
    /** True iff the key type is encoded in a fixed #of bytes. */
    public boolean isFixedLength() {
        
        return encodedLength != 0;
        
    }

    /**
     * The #of bytes in which values of that {@link KeyType} are encoded -or-
     * zero (0) iff values are encoded in a variable number of bytes with a
     * <code>nul</code> terminator for the byte sequence.
     */
    public int getEncodedLength() {

        return encodedLength;
        
    }
    
    /**
     * The byte that indicates this {@link KeyType}.
     * 
     * @return
     */
    public byte getByteCode() {

        return b;

    }

    /**
     * Return the {@link KeyType} given its byte code.
     * 
     * @param b
     *            The byte code.
     *            
     * @return The {@link KeyType}.
     */
    static public KeyType getKeyType(byte b) {

        switch (b) {
        case 0:
            return Integer;
        case 1:
            return Long;
        case 2:
            return Float;
        case 3:
            return Double;
        case 4:
            return Unicode;
        case 5:
            return ASCII;
        case 6:
            return Date;
//        case 7:
//            return UnsignedBytes;
        default:
            throw new IllegalArgumentException("byte=" + b);
        }

    }

}
