package it.unimi.dsi.io;

import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.io.OfflineIterable;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import junit.framework.TestCase;

public class OfflineIterableTest extends TestCase {

	public void doIt( String[] strings ) throws IOException {
		OfflineIterable.Serializer<MutableString,MutableString> stringSerializer = new OfflineIterable.Serializer<MutableString,MutableString>() {
			public void read( DataInputStream dis, MutableString x ) throws IOException {
				x.readSelfDelimUTF8( (InputStream)dis );
			}
			public void write( MutableString x, DataOutputStream dos ) throws IOException {
				x.writeSelfDelimUTF8( (OutputStream)dos );
			}
		};
		OfflineIterable<MutableString,MutableString> stringIterable = new OfflineIterable<MutableString,MutableString>( stringSerializer, new MutableString() );
		for ( String s: strings ) 
			stringIterable.add( new MutableString( s ) );
		ObjectIterator<String> shouldBe = ObjectIterators.wrap( strings );
		for ( MutableString m: stringIterable ) 
			assertEquals( new MutableString( shouldBe.next() ), m );
		assertFalse( shouldBe.hasNext() );
		stringIterable.close();
		stringIterable.close(); // Twice, to test for safety
	}
	
	public void testSimple() throws IOException {
		doIt( new String[] { "this", "is", "a", "test" } );
	}
	
	public void testEmpty() throws IOException {
		doIt( new String[ 0 ] );
	}
	
}
