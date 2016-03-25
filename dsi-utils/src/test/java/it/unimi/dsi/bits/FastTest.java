package it.unimi.dsi.bits;

import it.unimi.dsi.bits.Fast;
import junit.framework.TestCase;

public class FastTest extends TestCase {
	
	public void testCeilLog2() {
		for( int i = 1; i < 1000; i++ ) assertEquals( (int)Math.ceil( Math.log( i ) / Math.log( 2 ) ), Fast.ceilLog2( i ) );
	}

	public void testLength() {
		assertEquals( 1, Fast.length( 0 ) );
		assertEquals( 1, Fast.length( 0L ) );
		for( int i = 1; i < 100; i++ ) assertEquals( Fast.mostSignificantBit( i ) + 1, Fast.length( i ) ); 
		for( long i = 1; i < 100; i++ ) assertEquals( Fast.mostSignificantBit( i ) + 1, Fast.length( i ) ); 
	}
	
	public void testMostSignificantBit() {
		assertEquals( -1, Fast.mostSignificantBit( 0 ) );
		for( int i = 0; i < 64; i++ ) {
			assertEquals( i, Fast.mostSignificantBit( 1L << i ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i | 1 ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i | 1 ) );
		}
		
		for( long i = 1; i < ( 1L << 62 ); i += 10000000000000L )
			assertEquals( Long.toString( i ), 63 - Long.numberOfLeadingZeros( i ), Fast.mostSignificantBit( i ) );
	}

	public void testLeastSignificantBit() {
		assertEquals( -1, Fast.leastSignificantBit( 0 ) );
		for( int i = 0; i < 64; i++ ) {
			assertEquals( i, Fast.leastSignificantBit( 1L << i ) );
			assertEquals( 0, Fast.leastSignificantBit( 1L << i | 1 ) );
			assertEquals( i, Fast.leastSignificantBit( 1L << i ) );
			assertEquals( 0, Fast.leastSignificantBit( 1L << i | 1 ) );
		}
		
		for( long i = 1; i < ( 1L << 62 ); i += 10000000000000L )
			assertEquals( Long.toString( i ), Long.numberOfTrailingZeros( i ), Fast.leastSignificantBit( i ) );
	}

	public void testCount() {
		assertEquals( 0, Fast.count( 0 ) );
		assertEquals( 1, Fast.count( 1 ) );
		assertEquals( 64, Fast.count( 0xFFFFFFFFFFFFFFFFL ) );
		assertEquals( 32, Fast.count( 0xFFFFFFFFL ) );
		assertEquals( 32, Fast.count( 0xAAAAAAAAAAAAAAAAL ) );
	}

	public void testSelect() {
		assertEquals( 0, Fast.select( 1, 0 ) );
		for( int i = 0; i < 64; i++ ) assertEquals( i, Fast.select( 0xFFFFFFFFFFFFFFFFL, i ) );
		for( int i = 1; i < 32; i++ ) assertEquals( 2 * i + 1, Fast.select( 0xAAAAAAAAAAAAAAAAL, i ) );
	}
	

	public void testInt2Nat() {
		assertEquals( Integer.MAX_VALUE, Fast.int2nat( Integer.MIN_VALUE / 2 ) );
		assertEquals( Integer.MAX_VALUE - 2, Fast.int2nat( Integer.MIN_VALUE / 2 + 1 ) );
		assertEquals( Integer.MAX_VALUE - 1, Fast.int2nat( Integer.MAX_VALUE / 2 ) );

		for( int i = 0; i < 16; i++ ) assertEquals( 2*i, Fast.int2nat( i ) );
		for( int i = -16; i < 0; i++ ) assertEquals( -2*i-1, Fast.int2nat( i ) );

		assertEquals( Long.MAX_VALUE, Fast.int2nat( Long.MIN_VALUE / 2 ) );
		assertEquals( Long.MAX_VALUE - 2, Fast.int2nat( Long.MIN_VALUE / 2 + 1 ) );
		assertEquals( Long.MAX_VALUE - 1, Fast.int2nat( Long.MAX_VALUE / 2 ) );

		for( int i = 0; i < 16; i++ ) assertEquals( 2L*i, Fast.int2nat( i ) );
		for( int i = -16; i < 0; i++ ) assertEquals( -2L*i-1, Fast.int2nat( i ) );
	}

	public void testNat2Int() {
		assertEquals( Integer.MIN_VALUE / 2, Fast.nat2int( Integer.MAX_VALUE ) );
		assertEquals( Integer.MIN_VALUE / 2 + 1, Fast.nat2int( Integer.MAX_VALUE - 2 ) );
		assertEquals( Integer.MAX_VALUE / 2, Fast.nat2int( Integer.MAX_VALUE - 1 ) );

		for( int i = 0; i < 16; i++ ) assertEquals( i, Fast.nat2int( 2*i ) );
		for( int i = -16; i < 0; i++ ) assertEquals( i, Fast.nat2int( -2*i-1 ) );

		assertEquals( Long.MIN_VALUE / 2 + 1, Fast.nat2int( Long.MAX_VALUE - 2 ) );
		assertEquals( Long.MIN_VALUE / 2, Fast.nat2int( Long.MAX_VALUE ) );
		assertEquals( Long.MAX_VALUE / 2, Fast.nat2int( Long.MAX_VALUE - 1 ) );

		for( int i = 0; i < 16; i++ ) assertEquals( i, Fast.nat2int( 2L*i ) );
		for( int i = -16; i < 0; i++ ) assertEquals( i, Fast.nat2int( -2L*i-1 ) );

	}

	public void testLeastMostSignificantBit() {
		for( int i = 0; i < 32; i++ ) {
			assertEquals( i, Fast.leastSignificantBit( 1 << i ) );
			assertEquals( i, Fast.mostSignificantBit( 1 << i ) );
			assertEquals( 0, Fast.leastSignificantBit( 1 << i | 1 ) );
			assertEquals( i, Fast.mostSignificantBit( 1 << i | 1 ) );
			assertEquals( i, Fast.leastSignificantBit( 1L << i ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i ) );
			assertEquals( 0, Fast.leastSignificantBit( 1L << i | 1 ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i | 1 ) );
		}
		for( int i = 32; i < 64; i++ ) {
			assertEquals( i, Fast.leastSignificantBit( 1L << i ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i ) );
			assertEquals( 0, Fast.leastSignificantBit( 1L << i | 1 ) );
			assertEquals( i, Fast.mostSignificantBit( 1L << i | 1 ) );
		}
	}

}
