package it.unimi.dsi.util;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.util.BloomFilter;

import java.util.Random;

import junit.framework.TestCase;

public class BloomFilterTest extends TestCase {
	
	public void testAdd() {
		BloomFilter bloomFilter = new BloomFilter( 10, 30 ); // High precision 
		assertTrue( bloomFilter.add( "test" ) );
		assertFalse( bloomFilter.add( "test" ) );
		assertTrue( bloomFilter.add( "foo" ) );
		assertTrue( bloomFilter.add( "bar" ) );
		assertEquals( 3, bloomFilter.size() );
		
		bloomFilter.clear();
		assertTrue( bloomFilter.add( new int[] { 0, 1 } ) );
		assertFalse( bloomFilter.add( new int[] { 0, 1 } ) );
		assertTrue( bloomFilter.add( new int[] { 1, 2 } ) );
		assertTrue( bloomFilter.add( new int[] { 1, 0 } ) );
		assertEquals( 3, bloomFilter.size() );
	}

	public void testConflicts() {
		BloomFilter bloomFilter = new BloomFilter( 1000, 11 ); // Low precision
		LongOpenHashSet longs = new LongOpenHashSet();
		Random random = new Random( 0 );
		
		for( int i = 1000; i-- != 0; ) {
			final long l = random.nextLong();
			longs.add( l );
			bloomFilter.add( Long.toBinaryString( l ) );
		}
		
		assertEquals( longs.size(), bloomFilter.size() );
	}
}
