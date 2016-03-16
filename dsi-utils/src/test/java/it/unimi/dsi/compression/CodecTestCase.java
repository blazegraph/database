package it.unimi.dsi.compression;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.compression.Decoder;
import it.unimi.dsi.compression.PrefixCodec;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

public abstract class CodecTestCase extends TestCase {

	public void test() {}
	
	protected void checkPrefixCodec( PrefixCodec codec, Random r ) throws IOException {
		int[] symbol = new int[ 100 ];
		BooleanArrayList bits = new BooleanArrayList();
		for( int i = 0; i < symbol.length; i++ ) symbol[ i ] = r.nextInt( codec.size() ); 
		for( int i = 0; i < symbol.length; i++ ) {
			BitVector word = codec.codeWords()[ symbol[ i ] ];
			for( int j = 0; j < word.size(); j++ ) bits.add( word.get( j ) );
		}
	
		BooleanIterator booleanIterator = bits.iterator();
		Decoder decoder = codec.decoder();
		for( int i = 0; i < symbol.length; i++ ) {
			assertEquals( decoder.decode( booleanIterator ), symbol[ i ] );
		}
		
		FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream( fbaos, 0 );
		obs.write( bits.iterator() );
		obs.flush();
		InputBitStream ibs = new InputBitStream( fbaos.array );
		
		for( int i = 0; i < symbol.length; i++ ) {
			assertEquals( decoder.decode( ibs ), symbol[ i ] );
		}
	}

	protected void checkLengths( int[] frequency, int[] codeLength, BitVector[] codeWord ) {
		for( int i = 0; i < frequency.length; i++ ) 
			assertEquals( Integer.toString( i ), codeLength[ i ], codeWord[ i ].size() );
	}
}
