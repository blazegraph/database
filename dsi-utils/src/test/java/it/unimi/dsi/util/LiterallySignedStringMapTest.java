package it.unimi.dsi.util;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.LiterallySignedStringMap;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import junit.framework.TestCase;

public class LiterallySignedStringMapTest extends TestCase {
	
	private final static class CharSequenceStrategy implements Hash.Strategy<CharSequence>, Serializable {
		private static final long serialVersionUID = 1L;

		public boolean equals( CharSequence a, CharSequence b ) {
			return a.toString().equals( b.toString() );
		}

		public int hashCode( CharSequence o ) {
			return o.toString().hashCode();
		}
	}

	@SuppressWarnings("unchecked")
	public void testNumbers() throws IOException, ClassNotFoundException {
		for( int n = 10; n < 10000; n *= 10 ) {
			String[] s = new String[ n ];
			for( int i = s.length; i-- != 0; ) s[ i ] = Integer.toString( i );
			Collections.shuffle( Arrays.asList( s ) );

			FrontCodedStringList fcl = new FrontCodedStringList( Arrays.asList( s ), 8, true );
			// Test with mph
			Object2LongOpenCustomHashMap<CharSequence> mph = new Object2LongOpenCustomHashMap<CharSequence>( new CharSequenceStrategy());
			mph.defaultReturnValue( -1 );
			for( int i = 0; i < s.length; i++ ) mph.put( new MutableString( s[ i ] ),  i );
			
			LiterallySignedStringMap map = new LiterallySignedStringMap( mph, fcl );

			for( int i = s.length; i-- != 0; ) assertEquals( i, map.getLong( s[ i ] ) );
			for( int i = s.length + n; i-- != s.length; ) assertEquals( -1, map.getLong( Integer.toString( i ) ) );

			File temp = File.createTempFile( getClass().getSimpleName(), "test" );
			temp.deleteOnExit();
			BinIO.storeObject( map, temp );
			map = (LiterallySignedStringMap)BinIO.loadObject( temp );

			for( int i = s.length; i-- != 0; ) assertEquals( i, map.getLong( s[ i ] ) );
			for( int i = s.length + n; i-- != s.length; ) assertEquals( -1, map.getLong( Integer.toString( i ) ) );
		}
	}
}
