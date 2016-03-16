package it.unimi.dsi.lang;

import java.util.Set;

import it.unimi.dsi.lang.ObjectParser;
import junit.framework.TestCase;

public class ObjectParserTest extends TestCase {
	
	public void testObject() throws Exception {
		assertEquals( Object.class, ObjectParser.fromSpec( "java.lang.Object" ).getClass() );
		assertEquals( Object.class, ObjectParser.fromSpec( "java.lang.Object()" ).getClass() );

		assertEquals( Object.class, ObjectParser.fromSpec( "Object", Object.class, new String[] { "java.lang" } ).getClass() );
		assertEquals( Object.class, ObjectParser.fromSpec( "Object", Object.class, new String[] { "foo", "java.lang" } ).getClass() );
	}


	public void testString() throws Exception {
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String(foo)" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String(\"foo\")" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String( foo)" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String( \"foo\")" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String( foo )" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String( \"f\\oo\" )" ) );
		assertEquals( "f\\oo", ObjectParser.fromSpec( "java.lang.String( f\\oo )" ) );
		assertEquals( "foo", ObjectParser.fromSpec( "java.lang.String( \"foo\" )" ) );
		assertEquals( "fo\"o", ObjectParser.fromSpec( "java.lang.String(\"fo\\\"o\")" ) );
		
		boolean error = false;
		try {
			ObjectParser.fromSpec( "java.lang.String(\"fo\"o\")" );
		}
		catch( IllegalArgumentException thisIsWhatWeWant ) {
			error = true;
		}
		
		assertTrue( error );

		error = false;
		try {
			ObjectParser.fromSpec( "java.lang.String(fo" );
		}
		catch( IllegalArgumentException thisIsWhatWeWant ) {
			error = true;
		}
		
		assertTrue( error );


		error = false;
		try {
			ObjectParser.fromSpec( "java.lang.String()", Set.class );
		}
		catch( ClassCastException thisIsWhatWeWant ) {
			error = true;
		}
		
		assertTrue( error );
		
		assertEquals( "", ObjectParser.fromSpec( "java.lang.String()" ) );
		assertEquals( "", ObjectParser.fromSpec( "java.lang.String" ) );

		assertEquals( ")foo", ObjectParser.fromSpec( "java.lang.String()foo)" ) );
	}
	
	public void testTwoStrings() throws Exception {
		final Object context = new Object();
		
		assertEquals( new TwoStrings( "foo", "bar" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(\"foo\", \"bar\")" ));
		assertEquals( new TwoStrings( "foo", "bar" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(foo, bar)" ));
		assertEquals( new TwoStrings( "foo", "bar" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings( foo , bar )" ));
		assertEquals( new TwoStrings( "", "" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(,)" ));
		assertEquals( new TwoStrings( "", "" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings( , )" ));
		assertEquals( new TwoStrings( "", "" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings( ,\"\" )" ));

		assertEquals( new TwoStrings( "foo", "foo" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(foo)", Object.class, null, new String[] { "getInstance" } ));
		assertEquals( new TwoStrings( "3", "3" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(foo,bar,boo)", Object.class, null, new String[] { "getInstance" } ));
		assertEquals( new TwoStrings( "foo", "3" ), ObjectParser.fromSpec( "it.unimi.dsi.lang.TwoStrings(foo,bar,boo)", Object.class ));

		assertEquals( new TwoStrings( context, "foo", "bar" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(\"foo\", \"bar\")" ));
		assertEquals( new TwoStrings( context, "foo", "bar" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(foo, bar)" ));
		assertEquals( new TwoStrings( context, "foo", "bar" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings( foo , bar )" ));
		assertEquals( new TwoStrings( context, "", "" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(,)" ));
		assertEquals( new TwoStrings( context, "", "" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings( , )" ));
		assertEquals( new TwoStrings( context, "", "" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings( ,\"\" )" ));

		assertEquals( new TwoStrings( context, "foo", "foo" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(foo)", Object.class, null, new String[] { "getInstance" } ));
		assertEquals( new TwoStrings( context, "3", "3" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(foo,bar,boo)", Object.class, null, new String[] { "getInstance" } ));
		assertEquals( new TwoStrings( context, "foo", "3" ), ObjectParser.fromSpec( context, "it.unimi.dsi.lang.TwoStrings(foo,bar,boo)", Object.class ));
}
}
