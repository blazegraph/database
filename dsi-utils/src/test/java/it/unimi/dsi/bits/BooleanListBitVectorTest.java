package it.unimi.dsi.bits;

import it.unimi.dsi.bits.BooleanListBitVector;

import java.io.IOException;

import junit.framework.TestCase;

public class BooleanListBitVectorTest extends TestCase {

	public void testSetClearFlip() {
		BooleanListBitVector v = BooleanListBitVector.getInstance();
		v.size( 1 );
		BitVectorTestCase.testSetClearFlip( v );
		v.size( 64 );
		BitVectorTestCase.testSetClearFlip( v );
		v.size( 80 );
		BitVectorTestCase.testSetClearFlip( v );
		v.size( 150 );
		BitVectorTestCase.testSetClearFlip( v );
		
		BitVectorTestCase.testSetClearFlip( v.subVector( 0, 90 ) );
		BitVectorTestCase.testSetClearFlip( v.subVector( 5, 90 ) );
	}

	public void testFillFlip() {
		BooleanListBitVector v = BooleanListBitVector.getInstance();
		v.size( 100 );
		BitVectorTestCase.testFillFlip( v );
		BitVectorTestCase.testFillFlip( v.subVector( 0, 90 ) );
		BitVectorTestCase.testFillFlip( v.subVector( 5, 90 ) );
	}
	
	public void testRemove() {
		BitVectorTestCase.testRemove( BooleanListBitVector.getInstance() );
	}

	public void testAdd() {
		BitVectorTestCase.testAdd( BooleanListBitVector.getInstance() );
	}

	public void testCopy() {
		BitVectorTestCase.testCopy( BooleanListBitVector.getInstance() );
	}

	public void its() {
		BitVectorTestCase.its( BooleanListBitVector.getInstance() );
	}
		
	public void testLongBigListView() {
		BitVectorTestCase.testLongBigListView( BooleanListBitVector.getInstance() );
	}

	public void testLongSetView() {
		BitVectorTestCase.testLongSetView( BooleanListBitVector.getInstance() );
	}

	public void testFirstLast() {
		BitVectorTestCase.testFirstLastPrefix( BooleanListBitVector.getInstance() );
	}
	
	public void testLogicOperators() {
		BitVectorTestCase.testLogicOperators( BooleanListBitVector.getInstance() );
	}

	public void testCount() {
		BitVectorTestCase.testCount( BooleanListBitVector.getInstance() );
	}

	public void testSerialisation() throws IOException, ClassNotFoundException {
		BitVectorTestCase.testSerialisation( BooleanListBitVector.getInstance() );
	}

	public void testReplace() {
		BitVectorTestCase.testReplace( BooleanListBitVector.getInstance() );
	}

	public void testAppend() {
		BitVectorTestCase.testAppend( BooleanListBitVector.getInstance() );
	}
	
}
