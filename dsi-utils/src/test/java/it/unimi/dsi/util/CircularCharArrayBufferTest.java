package it.unimi.dsi.util;

import it.unimi.dsi.util.CircularCharArrayBuffer;

import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang.RandomStringUtils;

public class CircularCharArrayBufferTest extends TestCase {
	
	static Random r = new Random( 0 );
	static int[] sizes = { 1, 5, 10, 100, 500, 1000 };
	
	private static void copyInto( CircularFifoBuffer cfb, char[] c, int offset, int length ) {
		int howMany = Math.min( length, cfb.size() );
		Iterator<?> it = cfb.iterator();
		for ( int i = 0; i < howMany; i++ ) 
			c[ offset + i ] = ((Character)it.next()).charValue();
	}
	
	@SuppressWarnings("boxing")
	public void testAdd() {
		for ( int size: sizes ) {
			System.out.printf( "CIRCULAR BUFFER OF SIZE %d: ", size );
			CircularFifoBuffer cfb = new CircularFifoBuffer( size );
			CircularCharArrayBuffer ccab = new CircularCharArrayBuffer( size );
			int times = r.nextInt( 50 );
			System.out.println( times + " times" );
			for ( int j = 0; j < times; j++ ) {
				char[] c = new char[ 1 + r.nextInt( 1 + size * 10 / 2 ) ];
				int offset = r.nextInt( c.length );
				int len = r.nextInt( c.length - offset );
				System.arraycopy( RandomStringUtils.randomAlphanumeric( c.length ).toCharArray(), 0, c, 0, c.length );
				for ( int i = offset; i < offset + len; i++ )
					cfb.add( new Character( c[ i ] ) );
				ccab.add( c, offset, len );
				char[] res = new char[ cfb.size() ];
				copyInto( cfb, res, 0, cfb.size() );
				char[] res2 = new char[ cfb.size() ];
				ccab.toCharArray( res2, 0, cfb.size() );
				assertEquals( new String( res ), new String( res2 ) );
			}
		}
	}

}
