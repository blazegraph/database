package it.unimi.dsi.bits;

import it.unimi.dsi.bits.AbstractBitVector;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.BooleanListBitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import junit.framework.TestCase;

public class AbstractBitVectorTest extends TestCase {

	private final static class MinimalAlternatingBitVector extends AbstractBitVector {
		private long length = 129;

		public boolean getBoolean( long index ) { return index % 2 != 0; }
		public long length() { return length; }
		
		public MinimalAlternatingBitVector length( long newLength ) {
			this.length  = newLength;
			return this;
		}
	}

	public void testUnsupported() {
		final BitVector v = new MinimalAlternatingBitVector();

		v.getBoolean( 0 );
		v.length();
		
		boolean ok = false;
		try {
			v.removeBoolean( 0 );
		}
		catch( UnsupportedOperationException e ) {
			ok = true;
		}
		
		assertTrue( ok );
		
		ok = false;
		try {
			v.set( 0, 0 );
		}
		catch( UnsupportedOperationException e ) {
			ok = true;
		}
		
		assertTrue( ok );
		
		ok = false;
		try {
			v.add( 0, 0 );
		}
		catch( UnsupportedOperationException e ) {
			ok = true;
		}
		
		assertTrue( ok );

		v.length( 1L<<32 );

		ok = false;
		try {
			v.size();
		}
		catch( IllegalStateException e ) {
			ok = true;
		}
		
		assertTrue( ok );

		ok = false;
		try {
			v.asLongBigList( 1 ).size();
		}
		catch( IllegalStateException e ) {
			ok = true;
		}
		
		assertTrue( ok );
}
	
	public void testCopy() {
		assertEquals( new MinimalAlternatingBitVector(), new MinimalAlternatingBitVector().copy() );
		assertEquals( new MinimalAlternatingBitVector().subVector( 2, 20 ), new MinimalAlternatingBitVector().subVector( 2, 20 ).copy() );
		assertEquals( new MinimalAlternatingBitVector().subVector( 5, 12 ), new MinimalAlternatingBitVector().subVector( 2, 20 ).subVector( 3, 10 ) );
		assertEquals( new MinimalAlternatingBitVector().subVector( 5, 12 ), new MinimalAlternatingBitVector().subVector( 2, 20 ).subVector( 3, 10 ).copy() );
		assertEquals( new MinimalAlternatingBitVector().subList( 2, 20 ), new MinimalAlternatingBitVector().subList( 2, 20 ).copy() );
		assertEquals( new MinimalAlternatingBitVector().subList( 5, 12 ), new MinimalAlternatingBitVector().subList( 2, 20 ).subList( 3, 10 ) );
		assertEquals( new MinimalAlternatingBitVector().subList( 5, 12 ), new MinimalAlternatingBitVector().subList( 2, 20 ).subList( 3, 10 ).copy() );
	}
	
	public void testCount() {
		MinimalAlternatingBitVector v = new MinimalAlternatingBitVector();
		assertEquals( v.length() / 2, v.count() );
	}
	
	public void testRemove() {
		BitVectorTestCase.testRemove( new AbstractBitVector.SubBitVector( BooleanListBitVector.getInstance().length( 1000 ), 10, 100 ) );
	}

	public void testAdd() {
		BitVectorTestCase.testAdd( new AbstractBitVector.SubBitVector( BooleanListBitVector.getInstance().length( 1000 ), 10, 100 ) );
	}

	public void testCompareTo() {
		MinimalAlternatingBitVector v = new MinimalAlternatingBitVector();
		LongArrayBitVector w = LongArrayBitVector.copy( v );
		assertEquals( 0, w.compareTo( v ) );
		assertEquals( 0, v.compareTo( w ) );
		w.set( 100 );
		assertEquals( 1, w.compareTo( v ) );
		assertEquals( -1, v.compareTo( w ) );
		w = LongArrayBitVector.ofLength( 10 );
		assertEquals( -1, w.compareTo( v ) );
		assertEquals( 1, v.compareTo( w ) );
		w = LongArrayBitVector.of( 1 );
		assertEquals( 1, w.compareTo( v ) );
		assertEquals( -1, v.compareTo( w ) );
	}
}
