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
 * Created on Oct 28, 2005
 */
package com.bigdata.io;

import java.io.IOException;
import java.util.Random;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

import junit.framework.TestCase;

/**
 * Test suite for packing and unpacking unsigned long integers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLongPacker extends TestCase {

    /**
     * 
     */
    public TestLongPacker() {
        super();
    }

    /**
     * @param name
     */
    public TestLongPacker(String name) {
        super(name);
    }

    /**
     * Unpacks a long.
     * 
     * @param expected The expected long value.
     * 
     * @param packed The packed byte[].
     * 
     * @throws IOException
     *             If there was not enough data.
     * 
     * @throws junit.framework.AssertionFailedError
     *             If there is too much data.
     */

    public void doUnpackTest( long expected, byte[] packed )
    	throws IOException
    {
    
        DataInputBuffer dib = new DataInputBuffer(packed);
        
        long actual = dib.unpackLong();
        
        assertEquals( "value", expected, actual );
        
//        assertTrue( "Expecting EOF", dib.read() == -1 );
        try {
            dib.readByte();
            fail("Expecting: "+IOException.class);
        }
        catch(IOException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }

    /**
     * Given the first byte of a packed long value, return the #of bytes into which
     * that value was packed (including this one).
     * 
     * @param firstByte The first byte.
     * 
     * @return The #of bytes.  This is in the range [1:8] inclusive.
     */
    
    public int getNBytes( int firstByte ) {
        int nbytes;
        if( ( firstByte & 0x80 ) != 0 ) { // high bit is set.
            nbytes = 8;
        } else {
            nbytes = firstByte >> 4; // clear the high bit and right shift one nibble.
        }
        return nbytes;  
    }
    
    public void testNBytes()
    {
       
        // high bit is set - always 8 bytes.
        assertEquals( "nbytes", 8, getNBytes( 0x80 ) );
        assertEquals( "nbytes", 8, getNBytes( 0x81 ) );
        assertEquals( "nbytes", 8, getNBytes( 0x8e ) );
        assertEquals( "nbytes", 8, getNBytes( 0x8f ) );
        
        // high bit is NOT set. nbytes is the upper nibble.
        assertEquals( "nbytes", 1, getNBytes( 0x10 ) );
        assertEquals( "nbytes", 2, getNBytes( 0x20 ) );
        assertEquals( "nbytes", 3, getNBytes( 0x30 ) );
        assertEquals( "nbytes", 4, getNBytes( 0x40 ) );
        assertEquals( "nbytes", 5, getNBytes( 0x50 ) );
        assertEquals( "nbytes", 6, getNBytes( 0x60 ) );
        assertEquals( "nbytes", 7, getNBytes( 0x70 ) );
       
        // high bit is NOT set. nbytes is the upper nibble.
        assertEquals( "nbytes", 1, getNBytes( 0x11 ) );
        assertEquals( "nbytes", 2, getNBytes( 0x21 ) );
        assertEquals( "nbytes", 3, getNBytes( 0x31 ) );
        assertEquals( "nbytes", 4, getNBytes( 0x41 ) );
        assertEquals( "nbytes", 5, getNBytes( 0x51 ) );
        assertEquals( "nbytes", 6, getNBytes( 0x61 ) );
        assertEquals( "nbytes", 7, getNBytes( 0x71 ) );
       
        // high bit is NOT set. nbytes is the upper nibble.
        assertEquals( "nbytes", 1, getNBytes( 0x1f ) );
        assertEquals( "nbytes", 2, getNBytes( 0x2f ) );
        assertEquals( "nbytes", 3, getNBytes( 0x3f ) );
        assertEquals( "nbytes", 4, getNBytes( 0x4f ) );
        assertEquals( "nbytes", 5, getNBytes( 0x5f ) );
        assertEquals( "nbytes", 6, getNBytes( 0x6f ) );
        assertEquals( "nbytes", 7, getNBytes( 0x7f ) );
       
    }
    
    public void testUnpack()
    	throws IOException
    {

        // upper nibble is 1, so nbytes == 1 and the lower nibble is the value.
        doUnpackTest( 0x0, new byte[]{(byte)0x10} );
        doUnpackTest( 0x1, new byte[]{(byte)0x11} );
        doUnpackTest( 0x7, new byte[]{(byte)0x17} );
        doUnpackTest( 0xf, new byte[]{(byte)0x1f} );

        // upper nibble is 2, so nbytes == 2.
        doUnpackTest( 0xf00, new byte[]{(byte)0x2f, (byte)0x00} );
        doUnpackTest( 0xfa7, new byte[]{(byte)0x2f, (byte)0xa7} );
        doUnpackTest( 0xfa0, new byte[]{(byte)0x2f, (byte)0xa0} );
        doUnpackTest( 0xf07, new byte[]{(byte)0x2f, (byte)0x07} );
        
        // upper nibble is 3, so nbytes == 3.
        doUnpackTest( 0xcaa4d, new byte[]{(byte)0x3c, (byte)0xaa, (byte)0x4d} );
        
        // high bit only, lower nibble plus next seven bytes are the value.
        doUnpackTest( 0xaeede00a539271fL,
                	new byte[]{(byte)0x8a, (byte)0xee, (byte)0xde, (byte)0x00,
                	           (byte)0xa5, (byte)0x39, (byte)0x27, (byte)0x1f
        			  }
        	      );
        
    }

    public void doPackTest( long v, byte[] expected )
    	throws IOException
    {
        
        DataOutputBuffer dob = new DataOutputBuffer();
        
        final int nbytes = dob.packLong( v );
        
        final byte[] actual = dob.toByteArray();
        
        assertEquals( "nbytes", expected.length, nbytes );
        assertEquals( "nbytes", getNBytes(expected[0]), nbytes );
        assertEquals( "bytes", expected, actual );
        
    }

    public void test_getNibbleLength()
    {

        // Note: zero (0) is interpreted as being one nibble for our purposes.
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0x0 ) );
        
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0x1 ) );
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0x2 ) );
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0x7 ) );
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0x8 ) );
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0xe ) );
        assertEquals( "nibbles", 1, DataOutputBuffer.getNibbleLength( 0xf ) );
        
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x10 ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x11 ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x12 ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x17 ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x18 ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x1e ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x1f ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x7f ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0x8f ) );
        assertEquals( "nibbles", 2, DataOutputBuffer.getNibbleLength( 0xff ) );

        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0x100 ) );
        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0x101 ) );
        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0x121 ) );
        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0x1ee ) );
        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0x1ff ) );
        assertEquals( "nibbles", 3, DataOutputBuffer.getNibbleLength( 0xfff ) );
        
        assertEquals( "nibbles", 4, DataOutputBuffer.getNibbleLength( 0x1ff0 ) );
        assertEquals( "nibbles", 4, DataOutputBuffer.getNibbleLength( 0x7ff0 ) );
        assertEquals( "nibbles", 4, DataOutputBuffer.getNibbleLength( 0xfff0 ) );
        assertEquals( "nibbles", 4, DataOutputBuffer.getNibbleLength( 0xfff1 ) );

        assertEquals( "nibbles", 5, DataOutputBuffer.getNibbleLength( 0x12345 ) );
        assertEquals( "nibbles", 5, DataOutputBuffer.getNibbleLength( 0x54321 ) );

        assertEquals( "nibbles", 6, DataOutputBuffer.getNibbleLength( 0x123456 ) );
        assertEquals( "nibbles", 6, DataOutputBuffer.getNibbleLength( 0x654321 ) );

        assertEquals( "nibbles", 7, DataOutputBuffer.getNibbleLength( 0x1234567 ) );
        assertEquals( "nibbles", 7, DataOutputBuffer.getNibbleLength( 0x7654321 ) );

        /*
         * Note: At 8 nibbles we have 32 bits. When the high bit is one, this
         * MUST be expressed as a long (trailing 'L', NOT cast to a long) or it
         * will be interpreted as a negative integer and sign extended to a
         * negative long.
         */
        assertEquals( "nibbles", 8, DataOutputBuffer.getNibbleLength( 0x12345678L ) );
        assertEquals( "nibbles", 8, DataOutputBuffer.getNibbleLength( 0x87654321L ) ); 

        assertEquals( "nibbles", 9, DataOutputBuffer.getNibbleLength( 0x123456789L ) );
        assertEquals( "nibbles", 9, DataOutputBuffer.getNibbleLength( 0x987654321L ) ); 

        assertEquals( "nibbles", 10, DataOutputBuffer.getNibbleLength( 0x123456789aL ) );
        assertEquals( "nibbles", 10, DataOutputBuffer.getNibbleLength( 0xa987654321L ) ); 

        assertEquals( "nibbles", 11, DataOutputBuffer.getNibbleLength( 0x123456789abL ) );
        assertEquals( "nibbles", 11, DataOutputBuffer.getNibbleLength( 0xba987654321L ) ); 

        assertEquals( "nibbles", 12, DataOutputBuffer.getNibbleLength( 0x123456789abcL ) );
        assertEquals( "nibbles", 12, DataOutputBuffer.getNibbleLength( 0xcba987654321L ) ); 

        assertEquals( "nibbles", 13, DataOutputBuffer.getNibbleLength( 0x123456789abcdL ) );
        assertEquals( "nibbles", 13, DataOutputBuffer.getNibbleLength( 0xdcba987654321L ) ); 

        assertEquals( "nibbles", 14, DataOutputBuffer.getNibbleLength( 0x123456789abcdeL ) );
        assertEquals( "nibbles", 14, DataOutputBuffer.getNibbleLength( 0xedcba987654321L ) ); 

        assertEquals( "nibbles", 15, DataOutputBuffer.getNibbleLength( 0x123456789abcdefL ) );
        assertEquals( "nibbles", 15, DataOutputBuffer.getNibbleLength( 0xfedcba987654321L ) ); 

        assertEquals( "nibbles", 16, DataOutputBuffer.getNibbleLength( 0x1234567890abcdefL ) );
        assertEquals( "nibbles", 16, DataOutputBuffer.getNibbleLength( 0xfedcba0987654321L ) ); 

    }
    
    public void testPack()
    	throws IOException
    {

        // [0:15] should be packed into one byte.
        doPackTest( 0x0, new byte[]{(byte)0x10} );
        doPackTest( 0x1, new byte[]{(byte)0x11} );
        doPackTest( 0x2, new byte[]{(byte)0x12} );
        doPackTest( 0xe, new byte[]{(byte)0x1e} );
        doPackTest( 0xf, new byte[]{(byte)0x1f} );

        /*
         * 0x10 through 0xfff overflow the lower nibble, so the value is packed
         * into two bytes. the first byte has the header and the next three
         * nibbles are the value. This case is good for up to 2^12, since there
         * are three full nibbles to encode the value.
         */

        doPackTest( 0x10,  new byte[]{(byte)0x20, (byte)0x10 });
        doPackTest( 0x11,  new byte[]{(byte)0x20, (byte)0x11 });
        doPackTest( 0x1f,  new byte[]{(byte)0x20, (byte)0x1f });
        doPackTest( 0x20,  new byte[]{(byte)0x20, (byte)0x20 });
        doPackTest( 0xff,  new byte[]{(byte)0x20, (byte)0xff });
        doPackTest( 0x100, new byte[]{(byte)0x21, (byte)0x00 });
        doPackTest( 0x101, new byte[]{(byte)0x21, (byte)0x01 });
        doPackTest( 0x121, new byte[]{(byte)0x21, (byte)0x21 });
        doPackTest( 0x1ee, new byte[]{(byte)0x21, (byte)0xee });
        doPackTest( 0x1ff, new byte[]{(byte)0x21, (byte)0xff });
        doPackTest( 0xfff, new byte[]{(byte)0x2f, (byte)0xff });

        /*
         * 0x1000 through 0xfffff fit into one more byte.
         */

        doPackTest( 0x1000,  new byte[]{(byte)0x30, (byte)0x10, (byte)0x00 });
        doPackTest( 0x1234,  new byte[]{(byte)0x30, (byte)0x12, (byte)0x34 });
        doPackTest( 0x1fff,  new byte[]{(byte)0x30, (byte)0x1f, (byte)0xff });
        doPackTest( 0x54321, new byte[]{(byte)0x35, (byte)0x43, (byte)0x21 });
        doPackTest( 0xfffff, new byte[]{(byte)0x3f, (byte)0xff, (byte)0xff });
        
    }
        
    public static final long SIGN_MASK = 1L<<63;
    
    public void testHighBit() {
        assertTrue( "sign bit", ( -1L & SIGN_MASK ) != 0 );
        assertFalse( "sign bit", ( 0L & SIGN_MASK ) != 0 );
    }
    
    private interface LongGenerator
    {
        public long nextLong();
    }
    
    /**
     * All long values in sequence starting from the given start value
     * and using the given increment.
     * @author thompsonbry
     */
    private static class Sequence implements LongGenerator
    {
        
        long _start, _inc, _next;
        
        public Sequence( long start, long inc ) {
            _start = start;
            _inc   = inc;
            _next  = start;
        }
        
        public long nextLong() {
            long v = _next;
            _next += _inc;
            return v;
//            double d = rnd.nextGaussian();
////          if( d < 0 ) d = -d;
//          final long expected = (long) ( d * Long.MAX_VALUE );
        }
        
    }
 
    /**
     * Random long values (32 bits of random long), including negatives,
     * with a uniform distribution.
     * 
     * @author thompsonbry
     */
    private static class RandomLong implements LongGenerator
    {
        
        Random _rnd;
        
        public RandomLong( Random rnd ) {
            _rnd = rnd;
        }

        public long nextLong() {
            return _rnd.nextLong();
        }
        
    }

    /**
     * Run a large #of pack/unpack operations on a sequence of long values to
     * demonstrate correctness in that sequence.  The sequence is the long
     * values from -1 to 1M by one (dense coverage).
     * 
     * @throws IOException
     */

    public void testStressSequence() throws IOException {

        // dense coverage of the first 1M values.
        doStressTest( 1000000, new Sequence( -1, 1 ) );
        
    }
    
    /**
     * Run a large #of random pack/unpack operations to sample the space while
     * showing correctness on those samples.  The amount of compression due to
     * packing for this test is <em>very</em> small since all bits are equally
     * likely to be non-zero, so the #of bytes required on average to pack a
     * long value is 8. 
     * 
     * @throws IOException
     */
    
    public void testStressRandom() throws IOException {

        // test on 1M random long values.
        doStressTest( 1000000, new RandomLong( new Random() ) );
        
    }

    /**
     * Run a stress test.  Writes some information of possible interest onto
     * System.err.
     * 
     * @param ntrials #of trials.
     * 
     * @param g Generator for the long values.
     * 
     * @throws IOException
     */
    public void doStressTest( int ntrials, LongGenerator g ) throws IOException {
        
        long nwritten = 0L;

        long packlen = 0L;
        
        long minv = Long.MAX_VALUE, maxv = Long.MIN_VALUE;
        
        for( int i=0; i<ntrials; i++ ) {
            
            long expected = g.nextLong();
            
            if( expected < 0L ) {
         
                DataOutputBuffer dos = new DataOutputBuffer();
                
                try {
                    
                    dos.packLong( expected );
                    
                    fail( "Expecting rejection of negative value: val="+expected );
                    
                }
                
                catch( IllegalArgumentException ex ) {
                    
//                    System.err.println( "Ingoring expected exception: "+ex );
                    
                }
    
                
            } else {

                if( expected > maxv ) maxv = expected;
                if( expected < minv ) minv = expected;
                    
                DataOutputBuffer dos = new DataOutputBuffer();
                
                final int actualByteLength1 = dos.packLong( expected );

                byte[] packed = dos.toByteArray();
                
                final int actualByteLength2 = getNBytes( packed[ 0 ] );
            
                DataInputBuffer dis = new DataInputBuffer( packed );
                
                final long actual = dis.unpackLong();

                assertEquals( "trial="+i, expected, actual );
                
                assertEquals( "trial="+i+", v="+expected+", nbytes", actualByteLength1, actualByteLength2 );

                packlen += packed.length; // total #of packed bytes.
                nwritten++; // count #of non-negative random values.
                
            }
            
        }

        System.err.println( "\nWrote "+nwritten+" non-negative long values." );
        System.err.println( "minv="+minv+", maxv="+maxv );
        System.err.println( "#packed bytes       ="+packlen );
        System.err.println( "#bytes if not packed="+(nwritten * 8));
        long nsaved = ( nwritten * 8 ) - packlen;
        System.err.println ("#bytes saved        ="+nsaved);
        System.err.println( "%saved by packing   ="+nsaved/(nwritten*8f)*100+"%");
        
    }
    
    public static void assertEquals( String msg, byte[] expected, byte[] actual )
    {
        assertEquals( msg+": length", expected.length, actual.length );
        for( int i=0; i<expected.length; i++ ) {
            assertEquals( msg+": byte[i="+i+"]", expected[i], actual[i] );
        }
    }
    
}
