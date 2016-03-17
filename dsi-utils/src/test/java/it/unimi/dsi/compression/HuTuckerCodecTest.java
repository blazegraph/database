package it.unimi.dsi.compression;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.compression.HuTuckerCodec;

import java.io.IOException;
import java.util.Random;

public class HuTuckerCodecTest extends CodecTestCase {
	public void testOneSymbol() throws IOException {
		HuTuckerCodec codec = new HuTuckerCodec( new int[] { 1 } );
		assertEquals( 1, codec.codeWords().length );
		assertEquals( LongArrayBitVector.ofLength( 0 ), codec.codeWords()[ 0 ] );
		long seed = System.currentTimeMillis();
		System.err.println( seed );
		Random r = new Random( seed );
		checkPrefixCodec( codec, r );
	}

	public void testTwoEquiprobableSymbols() throws IOException {
		HuTuckerCodec codec = new HuTuckerCodec( new int[] { 1, 1 } );
		assertEquals( 2, codec.codeWords().length );
		assertEquals( LongArrayBitVector.ofLength( 1 ), codec.codeWords()[ 0 ] );
		BitVector v = LongArrayBitVector.ofLength( 1 );
		v.set( 0 );
		assertEquals( v, codec.codeWords()[ 1 ] );
		long seed = System.currentTimeMillis();
		System.err.println( seed );
		Random r = new Random( seed );
		checkPrefixCodec( codec, r );
	}

	public void testThreeNonequiprobableSymbols() throws IOException {
		HuTuckerCodec codec = new HuTuckerCodec( new int[] { 1, 2, 4 } );
		assertEquals( 3, codec.codeWords().length );
		BitVector v = LongArrayBitVector.ofLength( 2 );
		assertEquals( v, codec.codeWords()[ 0 ] );
		v.set( 1 );
		assertEquals( v, codec.codeWords()[ 1 ] );

		v = LongArrayBitVector.ofLength( 1 );
		v.set( 0 );
		assertEquals( v, codec.codeWords()[ 2 ] );
		long seed = System.currentTimeMillis();
		System.err.println( seed );
		Random r = new Random( seed );
		checkPrefixCodec( codec, r );
	}

	public void testRandomFrequencies() throws IOException {
		long seed = System.currentTimeMillis();
		System.err.println( seed );
		Random r = new Random( seed );
		int[] frequency = new int[ 100 ];
		for( int i = 0; i < frequency.length; i++ ) frequency[ i ] = r.nextInt( 1000 ); 
		HuTuckerCodec codec = new HuTuckerCodec( frequency );
		checkPrefixCodec( codec, r );
	}

	public void testRandomCodeLengths() throws IOException {
		int[] frequency = { 805, 1335, 6401, 7156, 7333, 10613, 10951, 11708, 12710, 12948, 13237, 13976, 20355, 20909, 22398, 26303, 26400, 28380, 28865, 30152, 31693, };
		int[] codeLength = { 7, 7, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3 };
		HuTuckerCodec codec = new HuTuckerCodec( frequency );
		checkLengths( frequency, codeLength, codec.codeWords() );
		checkPrefixCodec( codec, new Random() );
	}

	public void testExponentialCodeLengths() throws IOException {
		int[] frequency = { 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912, 1073741824 };
		int[] codeLength = { 30, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
		HuTuckerCodec codec = new HuTuckerCodec( frequency );
		checkLengths( frequency, codeLength, codec.codeWords() );
		checkPrefixCodec( codec, new Random() );
	}
	
}
