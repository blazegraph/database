package it.unimi.dsi.util;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

public class ShiftAddXorSignedStringMapTest extends TestCase {
	
	@SuppressWarnings("unchecked")
	public void testNumbers() throws IOException, ClassNotFoundException {
		
		for( int width = 16; width <= Long.SIZE; width += 8 ) {
			String[] s = new String[ 1000 ];
			long[] v = new long[ s.length ];
			for( int i = s.length; i-- != 0; ) s[ (int)( v[ i ] = i  )] = Integer.toString( i );

			// Test with mph
			Object2LongOpenHashMap<String> mph = new Object2LongOpenHashMap<String>( s, v );
			ShiftAddXorSignedStringMap map = new ShiftAddXorSignedStringMap( Arrays.asList( s ).iterator(), mph, width );

			for( int i = s.length; i-- != 0; ) assertEquals( i, map.getLong( Integer.toString( i ) ) );
			for( int i = s.length + 100; i-- != s.length; ) assertEquals( -1, map.getLong( Integer.toString( i ) ) );

			File temp = File.createTempFile( getClass().getSimpleName(), "test" );
			temp.deleteOnExit();
			BinIO.storeObject( map, temp );
			map = (ShiftAddXorSignedStringMap)BinIO.loadObject( temp );

			for( int i = s.length; i-- != 0; ) assertEquals( i, map.getLong( Integer.toString( i ) ) );
			for( int i = s.length + 100; i-- != s.length; ) assertEquals( -1, map.getLong( Integer.toString( i ) ) );
		
		}
	}
}
