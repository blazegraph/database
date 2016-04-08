package it.unimi.dsi.bits;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.BitVectors;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.OfflineIterable;
import junit.framework.TestCase;

public class BitVectorsTest extends TestCase {
	
	public void testReadWriteFast() throws IOException {
		final FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream( fbaos );
		final LongArrayBitVector labv = LongArrayBitVector.getInstance();
		final BitVector[] a = new BitVector[] { BitVectors.ZERO, BitVectors.ONE, BitVectors.EMPTY_VECTOR, 
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL }, 64 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAL }, 60 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL, 0xAAAAAAAAAAAAAAAAL }, 128 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL, 0xAAAAAAAAAAAAAAAL }, 124 ) };
		
		for( BitVector bv: a ) { 
			BitVectors.writeFast( bv, dos );
			dos.close();
			assertEquals( bv, BitVectors.readFast( new DataInputStream( new FastByteArrayInputStream( fbaos.array ) ) ) );
			fbaos.reset();
		}
		
		for( BitVector bv: a ) { 
			BitVectors.writeFast( bv, dos );
			dos.close();
			assertEquals( bv, BitVectors.readFast( new DataInputStream( new FastByteArrayInputStream( fbaos.array ) ), labv ) );
			fbaos.reset();
		}
	}

	public void testMakeOffline() throws IOException {
		final BitVector[] a = new BitVector[] { BitVectors.ZERO, BitVectors.ONE, BitVectors.EMPTY_VECTOR, 
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL }, 64 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAL }, 60 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL, 0xAAAAAAAAAAAAAAAAL }, 128 ),
				LongArrayBitVector.wrap( new long[] { 0xAAAAAAAAAAAAAAAAL, 0xAAAAAAAAAAAAAAAL }, 124 ) };

		OfflineIterable<BitVector,LongArrayBitVector> iterable = new OfflineIterable<BitVector, LongArrayBitVector>( BitVectors.OFFLINE_SERIALIZER, LongArrayBitVector.getInstance() );
		iterable.addAll( Arrays.asList( a ) );
		
		Iterator<LongArrayBitVector> iterator = iterable.iterator();
		for( int i = 0; i < a.length; i++ ) assertEquals( a[ i ], iterator.next() );
		assertFalse( iterator.hasNext() );
	}
}
