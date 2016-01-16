/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Oct 24, 2005
 */
package com.bigdata.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Packing utility for non-negative <code>long</code> values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LongPacker
{
    
    public LongPacker() {
        super();
    }
    
    /**
     * Packs a non-negative long value into the minimum #of bytes in which the
     * value can be represented and writes those bytes onto the output stream.
     * The first byte determines whether or not the long value was packed and,
     * if packed, how many bytes were required to represent the packed long
     * value. When the high bit of the first byte is a one (1), then the long
     * value could not be packed and the long value is found by clearing the
     * high bit and interpreting the first byte plus the next seven (7) bytes as
     * a long. Otherwise the next three (3) bits are interpreted as an unsigned
     * integer giving the #of bytes (nbytes) required to represent the packed
     * long value. To recover the long value the high nibble is cleared and the
     * first byte together with the next nbytes are interpeted as an unsigned
     * long value whose leading zero bytes were not written.
     * 
     * <pre>
     *    
     * [0|1|2|3|4|5|6|7]
     *  1 - - -	  nbytes = 8, clear high bit and interpret this plus the next 7 bytes as a long.
     *  0 1 1 1	  nbytes = 7, clear high nibble and interpret this plus the next 6 bytes as a long. 
     *  0 1 1 0	  nbytes = 6, clear high nibble and interpret this plus the next 5 bytes as a long. 
     *  0 1 0 1	  nbytes = 5, clear high nibble and interpret this plus the next 4 bytes as a long.
     *  0 1 0 0	  nbytes = 4, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 1	  nbytes = 3, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 0	  nbytes = 2, clear high nibble and interpret this plus the next byte as a long.
     *  0 0 0 1	  nbytes = 1, clear high nibble.  value is the low nibble.
     *  
     * </pre>
     */ 
    static public int packLong(final DataOutput os, final long v)
            throws IOException {
        /*
         * You can only pack non-negative long values with this method.
         */
        if( v < 0 ) {
            throw new IllegalArgumentException( "negative value: v="+v );
        }
        /*
         * If the high byte is non-zero then we will write the value as a normal
         * long and return nbytes == 8. This case handles large positive long
         * values.
         */
        if( ( v >> 56 ) != 0 ) {
            os.write( (byte)((0xff & (v >> 56))|0x80) ); // note: set the high bit.
            os.write( (byte)(0xff & (v >> 48)) );
            os.write( (byte)(0xff & (v >> 40)) );
            os.write( (byte)(0xff & (v >> 32)) );
            os.write( (byte)(0xff & (v >> 24)) );
            os.write( (byte)(0xff & (v >> 16)) );
            os.write( (byte)(0xff & (v >>  8)) );
            os.write( (byte)(0xff & v) );
            return 8;
        }
        // #of nibbles required to represent the long value.
        final int nnibbles = getNibbleLength( v );
        // Is [nnibbles] even? (If it is even then we need to pad out an extra zero
        // nibble in the first byte.)
        final boolean evenNibbleCount = ( nnibbles == ( ( nnibbles >> 1 ) << 1 ) );
        // #of bytes required to represent the long value (plus the header nibble).
        final int nbytes = ( ( nnibbles +1 ) >> 1 ) + (evenNibbleCount?1:0);
        
        int nwritten = 0;
        if( evenNibbleCount ) {
            /*
             * An even nibble count requires that we pad the low nibble of the
             * first byte with zeros.
             */
            // header byte. low nibble is empty.
            byte b = (byte) ( nbytes << 4 );
            os.write( b );
            nwritten++;
            // remaining bytes containing the packed value.
            for( int i=(nnibbles-2)<<2; i>=0; i-=8 ) {
                b = (byte) (0xff & (v >> i));
                os.write( b );
                nwritten++;
            }
        } else {
            /*
             * An odd nibble count means that we pack the first nibble of the
             * long value into the low nibble of the header byte. In this case
             * the first nibble will always be the low nibble of the first
             * non-zero byte in the long value (the high nibble of that byte
             * must be zero since there is an odd nibble count).
             */
            byte highByte = (byte) (0xff & (v >> ((nbytes-1)*8) ));
            byte b = (byte) ( ( nbytes << 4 ) | highByte );
            os.write( b );
            nwritten++;
            for( int i=(nnibbles-3)<<2; i>=0; i-=8 ) {
                b = (byte) (0xff & (v >> i));
                os.write( b );
                nwritten++;
            }
        }
        return nwritten;
    }

    /**
     * Packs a non-negative long value into the minimum #of bytes in which the
     * value can be represented and writes those bytes onto the output stream.
     * The first byte determines whether or not the long value was packed and,
     * if packed, how many bytes were required to represent the packed long
     * value. When the high bit of the first byte is a one (1), then the long
     * value could not be packed and the long value is found by clearing the
     * high bit and interpreting the first byte plus the next seven (7) bytes as
     * a long. Otherwise the next three (3) bits are interpreted as an unsigned
     * integer giving the #of bytes (nbytes) required to represent the packed
     * long value. To recover the long value the high nibble is cleared and the
     * first byte together with the next nbytes are interpeted as an unsigned
     * long value whose leading zero bytes were not written.
     * 
     * <pre>
     *    
     * [0|1|2|3|4|5|6|7]
     *  1 - - -   nbytes = 8, clear high bit and interpret this plus the next 7 bytes as a long.
     *  0 1 1 1   nbytes = 7, clear high nibble and interpret this plus the next 6 bytes as a long. 
     *  0 1 1 0   nbytes = 6, clear high nibble and interpret this plus the next 5 bytes as a long. 
     *  0 1 0 1   nbytes = 5, clear high nibble and interpret this plus the next 4 bytes as a long.
     *  0 1 0 0   nbytes = 4, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 1   nbytes = 3, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 0   nbytes = 2, clear high nibble and interpret this plus the next byte as a long.
     *  0 0 0 1   nbytes = 1, clear high nibble.  value is the low nibble.
     *  
     * </pre>
     */ 
    static public int packLong(final OutputStream os, final long v)
            throws IOException {
        /*
         * You can only pack non-negative long values with this method.
         */
        if( v < 0 ) {
            throw new IllegalArgumentException( "negative value: v="+v );
        }
        /*
         * If the high byte is non-zero then we will write the value as a normal
         * long and return nbytes == 8. This case handles large positive long
         * values.
         */
        if( ( v >> 56 ) != 0 ) {
            os.write( (byte)((0xff & (v >> 56))|0x80) ); // note: set the high bit.
            os.write( (byte)(0xff & (v >> 48)) );
            os.write( (byte)(0xff & (v >> 40)) );
            os.write( (byte)(0xff & (v >> 32)) );
            os.write( (byte)(0xff & (v >> 24)) );
            os.write( (byte)(0xff & (v >> 16)) );
            os.write( (byte)(0xff & (v >>  8)) );
            os.write( (byte)(0xff & v) );
            return 8;
        }
        // #of nibbles required to represent the long value.
        final int nnibbles = getNibbleLength( v );
        // Is [nnibbles] even? (If it is even then we need to pad out an extra zero
        // nibble in the first byte.)
        final boolean evenNibbleCount = ( nnibbles == ( ( nnibbles >> 1 ) << 1 ) );
        // #of bytes required to represent the long value (plus the header nibble).
        final int nbytes = ( ( nnibbles +1 ) >> 1 ) + (evenNibbleCount?1:0);
        
        int nwritten = 0;
        if( evenNibbleCount ) {
            /*
             * An even nibble count requires that we pad the low nibble of the
             * first byte with zeros.
             */
            // header byte. low nibble is empty.
            byte b = (byte) ( nbytes << 4 );
            os.write( b );
            nwritten++;
            // remaining bytes containing the packed value.
            for( int i=(nnibbles-2)<<2; i>=0; i-=8 ) {
                b = (byte) (0xff & (v >> i));
                os.write( b );
                nwritten++;
            }
        } else {
            /*
             * An odd nibble count means that we pack the first nibble of the
             * long value into the low nibble of the header byte. In this case
             * the first nibble will always be the low nibble of the first
             * non-zero byte in the long value (the high nibble of that byte
             * must be zero since there is an odd nibble count).
             */
            byte highByte = (byte) (0xff & (v >> ((nbytes-1)*8) ));
            byte b = (byte) ( ( nbytes << 4 ) | highByte );
            os.write( b );
            nwritten++;
            for( int i=(nnibbles-3)<<2; i>=0; i-=8 ) {
                b = (byte) (0xff & (v >> i));
                os.write( b );
                nwritten++;
            }
        }
        return nwritten;
    }

    /**
     * Narrow interface to support packing against different buffer classes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface IByteBuffer {
        /**
         * Relative <i>put</i> method for writing a byte[] on the buffer.
         * 
         * @param b
         *            The byte[].
         * @param off
         *            The offset of the first byte in <i>b</i> to be written on
         *            the buffer.
         * @param len
         *            The #of bytes in <i>b</i> to be written on the buffer.
         */
        void put(final byte[] b, final int off, final int len);
    }

    /**
     * Packs a non-negative long value into the minimum #of bytes in which the
     * value can be represented and writes those bytes onto the buffer.
     * The first byte determines whether or not the long value was packed and,
     * if packed, how many bytes were required to represent the packed long
     * value. When the high bit of the first byte is a one (1), then the long
     * value could not be packed and the long value is found by clearing the
     * high bit and interpreting the first byte plus the next seven (7) bytes as
     * a long. Otherwise the next three (3) bits are interpreted as an unsigned
     * integer giving the #of bytes (nbytes) required to represent the packed
     * long value. To recover the long value the high nibble is cleared and the
     * first byte together with the next nbytes are interpreted as an unsigned
     * long value whose leading zero bytes were not written.
     * 
     * <pre>
     *    
     * [0|1|2|3|4|5|6|7]
     *  1 - - -   nbytes = 8, clear high bit and interpret this plus the next 7 bytes as a long.
     *  0 1 1 1   nbytes = 7, clear high nibble and interpret this plus the next 6 bytes as a long. 
     *  0 1 1 0   nbytes = 6, clear high nibble and interpret this plus the next 5 bytes as a long. 
     *  0 1 0 1   nbytes = 5, clear high nibble and interpret this plus the next 4 bytes as a long.
     *  0 1 0 0   nbytes = 4, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 1   nbytes = 3, clear high nibble and interpret this plus the next 3 bytes as a long.
     *  0 0 1 0   nbytes = 2, clear high nibble and interpret this plus the next byte as a long.
     *  0 0 0 1   nbytes = 1, clear high nibble.  value is the low nibble.
     *  
     * </pre>
     * 
     * @param v The unsigned long value.
     * 
     * @return The #of bytes onto which the unsigned long value was packed.
     */
    static final public int packLong(final long v, final byte[] pbuf,
            final IByteBuffer buf) {
        
        /*
         * You can only pack non-negative long values with this method.
         */
        
        if (v < 0) {
            
            throw new IllegalArgumentException("negative value: v=" + v);
            
        }
        
        /*
         * If the high byte is non-zero then we will write the value as a normal
         * long and return nbytes == 8. This case handles large positive long
         * values.
         */
        if( ( v >> 56 ) != 0 ) {
            pbuf[0] = ( (byte)((0xff & (v >> 56))|0x80) ); // note: set the high bit.
            pbuf[1] = ( (byte)(0xff & (v >> 48)) );
            pbuf[2] = ( (byte)(0xff & (v >> 40)) );
            pbuf[3] = ( (byte)(0xff & (v >> 32)) );
            pbuf[4] = ( (byte)(0xff & (v >> 24)) );
            pbuf[5] = ( (byte)(0xff & (v >> 16)) );
            pbuf[6] = ( (byte)(0xff & (v >>  8)) );
            pbuf[7] = ( (byte)(0xff & v) );
            buf.put(pbuf, 0, 8);
            return 8;
        }
        
        // #of nibbles required to represent the long value.
        final int nnibbles = LongPacker.getNibbleLength( v );

        /*
         * Is [nnibbles] even? (If it is even then we need to pad out an extra
         * zero nibble in the first byte.)
         */
        final boolean evenNibbleCount = ( nnibbles == ( ( nnibbles >> 1 ) << 1 ) );
        
        // #of bytes required to represent the long value (plus the header nibble).
        final int nbytes = ( ( nnibbles +1 ) >> 1 ) + (evenNibbleCount?1:0);
        
        int nwritten = 0;
        
        if( evenNibbleCount ) {
            
            /*
             * An even nibble count requires that we pad the low nibble of the
             * first byte with zeros.
             */
        
            // header byte. low nibble is empty.
            byte b = (byte) ( nbytes << 4 );
            
            pbuf[nwritten++] = b;
            
            // remaining bytes containing the packed value.
            for( int i=(nnibbles-2)<<2; i>=0; i-=8 ) {
                
                b = (byte) (0xff & (v >> i));
                
                pbuf[nwritten++] = b;
                
            }
            
        } else {
            
            /*
             * An odd nibble count means that we pack the first nibble of the
             * long value into the low nibble of the header byte. In this case
             * the first nibble will always be the low nibble of the first
             * non-zero byte in the long value (the high nibble of that byte
             * must be zero since there is an odd nibble count).
             */
            
            byte highByte = (byte) (0xff & (v >> ((nbytes-1)*8) ));
            
            byte b = (byte) ( ( nbytes << 4 ) | highByte );
            
            pbuf[nwritten++] = b;
            
            for( int i=(nnibbles-3)<<2; i>=0; i-=8 ) {
            
                b = (byte) (0xff & (v >> i));
                
                pbuf[nwritten++] = b;
                
            }
            
        }
        
        buf.put(pbuf,0,nwritten);
        
        return nwritten;
        
    }

    /**
     * Read a byte from an {@link InputStream} ala {@link DataInput#readByte()}
     * 
     * @param is
     *            The input stream.
     * @return The byte.
     * @throws IOException
     * @throws EOFException
     *             if the end of the file has been reached.
     */
    private static byte readByte(final InputStream is) throws IOException {
        
        final int v = is.read();
        
        if (v == -1)
            throw new EOFException();
        
        return (byte) (v & 0xff);
        
    }

    /**
     * Return the #of non-zero nibbles, counting from the first non-zero nibble
     * in the long value. A value of <code>0L</code> is considered to be one
     * nibble for our purposes.
     * 
     * @param v
     *            The long value.
     * 
     * @return The #of nibbles in [1:16].
     */
    static public int getNibbleLength( final long v )
    {
        for( int i=56, j=16; i>=0; i-=8, j-=2 ) {
            if( (0xf0 & (v >> i)) != 0 ) return j;
            if( (0x0f & (v >> i)) != 0 ) return j-1;
        }
        if( v != 0 ) throw new AssertionError( "v="+v );
        return 1; // value is zero, which is considered to be one nibble for our purposes.
    }

    /**
     * Return the #of non-zero bytes in the packed long value.  
     * 
     * @param v
     *            The long value.
     * 
     * @return The #bytes in [1:8].
     */
    static public int getByteLength(final long v) {

        // #of nibbles required to represent the long value.
        final int nnibbles = getNibbleLength( v );
        // Is [nnibbles] even? (If it is even then we need to pad out an extra zero
        // nibble in the first byte.)
        final boolean evenNibbleCount = ( nnibbles == ( ( nnibbles >> 1 ) << 1 ) );
        // #of bytes required to represent the long value (plus the header nibble).
        final int nbytes = ( ( nnibbles +1 ) >> 1 ) + (evenNibbleCount?1:0);
        
        return nbytes;
        
    }
    
    /**
     * Unpack a long value from the input stream.
     * 
     * @param is The input stream.
     * 
     * @return The long value.
     * 
     * @throws IOException
     */
    static public long unpackLong( final DataInput is ) throws IOException
    {
        int b = is.readByte();
        int nbytes;
        long l;
        if( ( b & 0x80 ) != 0 ) {
            // high bit is set.
            nbytes = 8; // use 8 bytes (this one plus the next 7).
            l = b & 0x7f; // clear the high bit - the rest of the byte is the start value.
        } else {
            // high bit is clear.
            nbytes = b >> 4; // nbytes is the upper nibble. (right shift one nibble).
            l = b & 0x0f; // starting value is lower nibble (clear the upper nibble).
        }
        for( int i=1; i<nbytes; i++ ) {
            // Read the next byte.
            b = is.readByte(); // readByte( is );
            // Shift the existing value one byte left and add into the low (unsigned) byte.
            l = (l << 8) + (0xff & b);
        }
        return l;
    }

    /**
     * Unpack a long value from the input stream.
     * 
     * @param is The input stream.
     * 
     * @return The long value.
     * 
     * @throws IOException
     */
    static public long unpackLong( final InputStream is ) throws IOException
    {
        int b = readByte(is);
        int nbytes;
        long l;
        if( ( b & 0x80 ) != 0 ) {
            // high bit is set.
            nbytes = 8; // use 8 bytes (this one plus the next 7).
            l = b & 0x7f; // clear the high bit - the rest of the byte is the start value.
        } else {
            // high bit is clear.
            nbytes = b >> 4; // nbytes is the upper nibble. (right shift one nibble).
            l = b & 0x0f; // starting value is lower nibble (clear the upper nibble).
        }
        for( int i=1; i<nbytes; i++ ) {
            // Read the next byte.
            b = readByte(is); // readByte( is );
            // Shift the existing value one byte left and add into the low (unsigned) byte.
            l = (l << 8) + (0xff & b);
        }
        return l;
    }

    /**
     * Unpack a long value from the buffer position.
     * 
     * @param buf
     *            The buffer
     * @param off
     *            The offset from which the data will be unpacked.
     * @return The long value.
     */
    static final public long unpackLong(final byte[] buf, int off) {
        int b = buf[off++];
        int nbytes;
        long l;
        if ((b & 0x80) != 0) {
            // high bit is set.
            nbytes = 8; // use 8 bytes (this one plus the next 7).
            l = b & 0x7f; // clear the high bit - the rest of the byte is the
                            // start value.
        } else {
            // high bit is clear.
            nbytes = b >> 4; // nbytes is the upper nibble. (right shift one
                                // nibble).
            l = b & 0x0f; // starting value is lower nibble (clear the upper
                            // nibble).
        }
        for (int i = 1; i < nbytes; i++) {
            // Read the next byte.
            b = buf[off++];
            // Shift the existing value one byte left and add into the low
            // (unsigned) byte.
            l = (l << 8) + (0xff & b);
        }
        return l;
    }

    /**
     * Convenience method unpacks long and throws an exception if the value
     * exceeds {@link Integer#MAX_VALUE}.
     * 
     * @param is
     *            The input stream.
     *            
     * @return The integer value.
     * 
     * @throws IOException
     */
    static public int unpackInt(final DataInput is) throws IOException {

        final long v = unpackLong(is);

        if (v > Integer.MAX_VALUE)
            throw new IOException();

        return (int) v;

    }

    /**
     * Convenience method unpacks long and throws an exception if the value
     * exceeds {@link Integer#MAX_VALUE}.
     * 
     * @param is
     *            The input stream.
     *            
     * @return The integer value.
     * 
     * @throws IOException
     */
    static public int unpackInt(final InputStream is) throws IOException {

        final long v = unpackLong(is);

        if (v > Integer.MAX_VALUE)
            throw new IOException();

        return (int) v;

    }

}
