/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package com.bigdata.btree;

/**
 * Miscelaneous conversion utility methods.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id$
 */
public class Conversion
{
    public static final long MAX_PACKEDLONGVALUE = 0x3fffffffffffffffL;

    /**
     * Convert a string into a byte array.
     */
    public static byte[] convertToByteArray( String s )
    {
        try {
            // see the following page for character encoding
            // http://java.sun.com/products/jdk/1.1/docs/guide/intl/encoding.doc.html
            return s.getBytes( "UTF8" );
        } catch ( java.io.UnsupportedEncodingException uee ) {
            uee.printStackTrace();
            throw new Error( "Platform doesn't support UTF8 encoding" );
        }
    }


    /**
     * Convert a byte into a byte array.
     */
    public static byte[] convertToByteArray( byte n )
    {
        n = (byte)( n ^ ( (byte) 0x80 ) ); // flip MSB because "byte" is signed
        return new byte[] { n };
    }


    /**
     * Convert a short into a byte array.
     */
    public static byte[] convertToByteArray( short n )
    {
        n = (short) ( n ^ ( (short) 0x8000 ) ); // flip MSB because "short" is signed
        byte[] key = new byte[ 2 ];
        pack2( key, 0, n );
        return key;
    }


    /**
     * Convert an int into a byte array.
     */
    public static byte[] convertToByteArray( int n )
    {
        n = (n ^ 0x80000000); // flip MSB because "int" is signed
        byte[] key = new byte[4];
        pack4(key, 0, n);
        return key;
    }


    /**
     * Convert a long into a byte array.
     */
    public static byte[] convertToByteArray( long n )
    {
        n = (n ^ 0x8000000000000000L); // flip MSB because "long" is signed
        byte[] key = new byte[8];
        pack8( key, 0, n );
        return key;
    }


    /**
     * Convert a byte array (encoded as UTF-8) into a String
     */
    public static String convertToString( byte[] buf )
    {
        return convertToString( buf, 0, buf.length );
    }

    /**
     * Convert a byte array (encoded as UTF-8) into a String
     */
    public static String convertToString( byte[] buf , int off, int len )
    {
        try {
            // see the following page for character encoding
            // http://java.sun.com/products/jdk/1.1/docs/guide/intl/encoding.doc.html
            return new String( buf, off, len, "UTF8" );
        } catch ( java.io.UnsupportedEncodingException uee ) {
            uee.printStackTrace();
            throw new Error( "Platform doesn't support UTF8 encoding" );
        }
    }

    /**
     * Convert a byte array into a short integer (signed 16-bit) value.
     */
    public static short convertToShort( byte[] buf )
    {
        short value = unpack2( buf, 0 );
        value = (short)( value ^ 0x8000 ); // flip MSB because "short" is signed
        return value;
    }


    /**
     * Convert a byte array into an integer (signed 32-bit) value.
     */
    public static int convertToInt( byte[] buf )
    {
        int value = unpack4( buf, 0 );
        value = ( value ^ 0x80000000 ); // flip MSB because "int" is signed
        return value;
    }


    /**
     * Convert a byte array into a long (signed 64-bit) value.
     */
    public static long convertToLong( byte[] buf )
    {
        long value = ( (long) unpack4( buf, 0 ) << 32  )
                     + ( unpack4( buf, 4 ) & 0xFFFFFFFFL );
        value = ( value ^ 0x8000000000000000L ); // flip MSB because "long" is signed
        return value;
    }




    static final short unpack2( byte[] buf, int offset)
    {
        short val = (short)((buf[offset] << 8)
            | ( ( buf[ offset+1 ] << 0 ) & 0x00FF ));
        
        return val;
    }

    static int unpack4( byte[] buf, int offset )
    {
        int value = ( buf[ offset ] << 24 )
            | ( ( buf[ offset+1 ] << 16 ) & 0x00FF0000 )
            | ( ( buf[ offset+2 ] << 8 ) & 0x0000FF00 )
            | ( ( buf[ offset+3 ] << 0 ) & 0x000000FF );

        return value;
    }

    public static long unpack8( byte[] buf, int offset )
    {
        long value = ( (long) unpack4( buf, offset ) << 32  )
                     + ( unpack4( buf, offset + 4 ) & 0xFFFFFFFFL );
        value = ( value ^ 0x8000000000000000L ); // flip MSB because "long" is signed
        return value;
    }

    static final void pack2( byte[] data, int offs, int val )
    {
        data[offs++] = (byte) ( val >> 8 );
        data[offs++] = (byte) val;
    }


    static final void pack4( byte[] data, int offs, int val )
    {
        data[offs++] = (byte) ( val >> 24 );
        data[offs++] = (byte) ( val >> 16 );
        data[offs++] = (byte) ( val >> 8 );
        data[offs++] = (byte) val;
    }


    static final void pack8( byte[] data, int offs, long val )
    {
        pack4( data, offs, (int) ( val >> 32 ) );
        pack4( data, offs + 4, (int) val );
    }

    
    /**
     * Used to compute the size of output buffers for storing packed values.
     * If you are storing more than one packed long, call this routine several times
     * to determine the final output buffer size, then call @link #convertToPackedByteArray(long, byte[], int)
     * multiple times with varying values for start.
     * @param n The value that will be stored
     * @return The number of bytes required to store 'n' in packed format
     */
    static public final int getRequiredPackedBufferSize (long n) {
        if (n <= 0x7fff)
            return 2;
        if (n <= 0x3fffffff)
            return 4;
        return 8;
    }
    
    /**
     * Used when reading packed long values from a stream.  Call this method on the first
     * byte of the packed long and it will return the total #of bytes in which the long
     * value was packed.
     * 
     * @param first The first byte of the packed long.
     * 
     * @return The #of bytes in which the long value was packed.
     */
    static public int getPackedByteCount( byte first )
    {
        if ((0x80 & first) == 0)
            return 2;
        if ((0xc0 & first) == 0x80){
            return 4;
        }
        return 8;
    }
    
    /**
     * Converts the specified value into a packed byte representation and stores the results
     * in the provided buffer.
     * 
     * Packing is accomplished by using the first 2 bits of a stored value to determine the total
     * number of bytes consumed by the stored value.
     * 
     *   The most significant two bits can contain the following values:
     *   <ul>
     *     <li>00 - the stored value is only 2 bytes wide, strip the first 1 bit</li>
     *     <li>01 - the stored value is only 2 bytes wide, strip the first 1 bit</li>
     *     <li>10 - the stored value is 4 bytes wide, strip the first 2 bits</li>
     *     <li>11 - the stored value is 8 bytes wide, strip the first 2 bits</li>
     *   </ul>
     * 
     * @param n The value that will be stored
     * @param out The output buffer (there must be room for the number of bytes returned by @link getRequiredPackedBufferSize(long) )
     * @param start The location in the output buffer to store the packed data 
     * @return The number of bytes in @param out that were actually filled in
     * @exception IllegalArgumentException if the value would be out of the range which
     * can be represented.
     */
    static public final int convertToPackedByteArray(long n, byte[] out, int start){
        if (n < 0 || n > MAX_PACKEDLONGVALUE) throw new IllegalArgumentException("Stored long value " + n + " is not >=0 and <= " + MAX_PACKEDLONGVALUE);

        if (n <= 0x7fff){
            pack2(out, start, (short)n);
            return 2;
        }
        
        else if (n <= 0x3fffffff){
            pack4(out, start, 0x80000000 | (int)n);
            return 4;
        }
        
        else {
            pack8(out, start, 0xc000000000000000L | n);
            return 8;
        }

    }
    
    /**
     * A convenience function to obtain the packed byte
     * array for a single long.  If multiple longs are being stored, use
     * @link #getRequiredPackedBufferSize() and @link #convertToPackedByteArray(long,byte[],int)
     * instead.
     * 
     * @param n
     * @return
     */
    static public final byte[] convertToPackedByteArray(long n){
        int siz = getRequiredPackedBufferSize(n);
        byte[] buf = new byte[siz];
        convertToPackedByteArray(n, buf, 0);
        return buf;
    }
    
    /**
     * A convenience function to obtain a long from it's packed byte array
     * representation.
     * 
     * @param b The packed byte array representation
     * @return The result of unpacking @param b
     */
    static public final long recoverLongFromPacked(byte[] b){
        return recoverLongFromPacked(b, 0);
    }
    
    /**
     * Recovers a long from a packed byte array.  If this method is being called
     * multiple times against the same input buffer, use @link #getRequiredPackedBufferSize(long)
     * to determine the next start location based on the returned value.
     * 
     * @param b The input buffer that contains the long value in a packed byte representation
     * @param start The offset in the input buffer to process
     * @return The recovered long value
     */
    static public final long recoverLongFromPacked(byte[] b, int start){
        // The most significant two bits can contain the following values
        // 00 - the value is only 2 bytes wide, strip the first 1 bit
        // 01 - the value is only 2 bytes wide, strip the first 1 bit
        // 10 - the value is 4 bytes wide, strip the first 2 bits
        // 11 - the value is 8 bytes wide, strip the first 2 bits
        
        byte first = b[start];
        if ((0x80 & first) == 0)
            return 0x7fff & unpack2(b, start);
        if ((0xc0 & first) == 0x80){
            return 0x3fffffff & unpack4(b, start);
        }
        return 0x3fffffffffffffffL & unpack8(b, start);
        
    }

}
