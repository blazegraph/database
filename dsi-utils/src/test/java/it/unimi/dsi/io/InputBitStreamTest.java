package it.unimi.dsi.io;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

public class InputBitStreamTest extends TestCase {

	public void testReadAligned() throws IOException {
		byte[] a = { 1 }, A = new byte[ 1 ];
		new InputBitStream( a ).read( A, 8 );
		assertTrue( Arrays.toString( a ) + " != " + Arrays.toString( A ), Arrays.equals( a, A ) );
		byte[] b = { 1, 2 }, B = new byte[ 2 ];
		new InputBitStream( b ).read( B, 16 );
		assertTrue( Arrays.toString( b ) + " != " + Arrays.toString( B ), Arrays.equals( b, B ) );
		byte[] c = { 1, 2, 3 }, C = new byte[ 3 ];
		new InputBitStream( c ).read( C, 24 );
		assertTrue( Arrays.toString( c ) + " != " + Arrays.toString( C ), Arrays.equals( c, C ) );
	}
	
	public void testOverflow() throws IOException {
		InputBitStream ibs = new InputBitStream( new byte[ 0 ] );
		ibs.readInt( 0 );
	}
	
	/**
     * Test operations on a byte[].
     * @throws IOException
     */
    public void testByteArray() throws IOException {
        
        final byte[] a = new byte[] { 0, 1, 2, 3,
                4, 5, 6, 7, 8, 9 };
        
        final InputBitStream ibs = new InputBitStream(a);
        
        assertEquals(0, ibs.readBits());
        assertEquals(0, ibs.readInt(8/* nbits */));
        assertEquals(8, ibs.readBits());
        
        assertEquals(1, ibs.readInt(8/* nbits */));
        assertEquals(16, ibs.readBits());
        
        assertEquals(2, ibs.readInt(8/* nbits */));
        assertEquals(24, ibs.readBits());
        
        assertEquals(3, ibs.readInt(8/* nbits */));
        assertEquals(32, ibs.readBits());
        
        ibs.position(2 << 3);
        assertEquals(2, ibs.readInt(8/* nbits */));
        
        try {
            ibs.position(-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            // Expected exception.
            System.err.println("Ignoring expected exception: "+ex);
        }

        try {
            ibs.position(a.length<<3+1);
            fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
            // Expected exception.
            System.err.println("Ignoring expected exception: "+ex);
        }


    }
    
	/**
	 * Test operations on a slice of a byte[].
	 * @throws IOException
	 */
	public void testByteArraySlice() throws IOException {
        
	    final byte[] a = new byte[] { 0, 1, 2, 3,
                4, 5, 6, 7, 8, 9 };
	    
        final InputBitStream ibs = new InputBitStream(a, 2/*off*/, 6/*len*/);
        
        assertEquals(0, ibs.readBits());
        assertEquals(2, ibs.readInt(8/* nbits */));
        assertEquals(8, ibs.readBits());
        
        assertEquals(3, ibs.readInt(8/* nbits */));
        assertEquals(16, ibs.readBits());

        // verify position() rewinds relative to the given offset.
        ibs.position(0);
        assertEquals(2, ibs.readInt(8/* nbits */));

        // verify illegal to position past the specified slice length.
        try {
            ibs.position(6<<3+1);
            fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
            // Expected exception.
            System.err.println("Ignoring expected exception: "+ex);
        }

    }
	
}
