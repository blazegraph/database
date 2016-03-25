package it.unimi.dsi.bits;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.bits.TransformationStrategy;
import junit.framework.TestCase;

public class PrefixFreeTransformationStrategyTest extends TestCase {

	public void testGetBoolean() {
		LongArrayBitVector v = LongArrayBitVector.of( 0, 1, 0 );
		TransformationStrategy<BitVector> prefixFree = TransformationStrategies.prefixFree();
		BitVector p = prefixFree.toBitVector( v );
		assertTrue( p.getBoolean( 0 ) );
		assertFalse( p.getBoolean( 1 ) );
		assertTrue( p.getBoolean( 2 ) );
		assertTrue( p.getBoolean( 3 ) );
		assertTrue( p.getBoolean( 4 ) );
		assertFalse( p.getBoolean( 5 ) );
		assertFalse( p.getBoolean( 6 ) );
		assertEquals( LongArrayBitVector.of(  1, 0, 1, 1, 1, 0, 0 ), p );
	}

	public void testGetLong() {
		LongArrayBitVector v = LongArrayBitVector.getInstance();
		v.append( 0xFFFFFFFFL, 32 );
		TransformationStrategy<BitVector> prefixFree = TransformationStrategies.prefixFree();
		BitVector p = prefixFree.toBitVector( v );
		assertEquals( 0xFFFFFFFFFFFFFFFFL, p.getLong( 0, 64 ) );
		assertFalse( p.getBoolean( 64 ) );

		v.clear();
		v.append( 0x0, 32 );
		assertEquals( 0x5555555555555555L, p.getLong( 0, 64 ) );
		assertFalse( p.getBoolean( 64 ) );

		v.clear();
		v.append( 0x3, 32 );
		assertEquals( 0x555555555555555FL, p.getLong( 0, 64 ) );
		assertEquals( 0x5FL, p.getLong( 0, 7 ) );
	}

}
