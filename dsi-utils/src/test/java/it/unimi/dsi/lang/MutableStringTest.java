package it.unimi.dsi.lang;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.lang.MutableString;
import junit.framework.TestCase;

public class MutableStringTest extends TestCase {

	public void testSqueezeSpace() {
		MutableString s = new MutableString( new char[] { 32, 13, 10, 32, 32, 32, 13, 10, 32, 32, 32, 13, 10, 32, 32, 32, 32, 32 } );
				
		assertEquals( new MutableString( " \r\n \r\n \r\n " ), s.squeezeSpace() );
		assertEquals( new MutableString( " " ), s.squeezeWhitespace() );
	}
	
	public void testSubsequence() {
		MutableString s = new MutableString( "abc" );
		CharSequence ss = s.subSequence( 1, 3 );
		assertEquals( new MutableString( "bc" ), ss );
		assertEquals( 1, ss.subSequence( 1, 2 ).length() );
	}
	
	public void testSkipSelfDelimUTF8() throws IOException {
		final FastByteArrayOutputStream fastByteArrayOutputStream = new FastByteArrayOutputStream();
		new MutableString( "a" ).writeSelfDelimUTF8( fastByteArrayOutputStream );
		new MutableString( "b" ).writeSelfDelimUTF8( fastByteArrayOutputStream );
		new MutableString( "\u221E" ).writeSelfDelimUTF8( fastByteArrayOutputStream );
		new MutableString( "c" ).writeSelfDelimUTF8( fastByteArrayOutputStream );
		fastByteArrayOutputStream.flush();
		final FastByteArrayInputStream fastByteArrayInputStream = new FastByteArrayInputStream( fastByteArrayOutputStream.array );
		assertEquals( "a", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
		assertEquals( "b", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
		assertEquals( 1, MutableString.skipSelfDelimUTF8( fastByteArrayInputStream ) );
		assertEquals( "c", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
		fastByteArrayInputStream.position( 0 );
		assertEquals( "a", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
		assertEquals( 1, MutableString.skipSelfDelimUTF8( fastByteArrayInputStream ) );
		assertEquals( "\uu221E", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
		assertEquals( "c", new MutableString().readSelfDelimUTF8( fastByteArrayInputStream ).toString() );
	}
}
