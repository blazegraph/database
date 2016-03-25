package it.unimi.dsi.util;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.StringReader;

import junit.framework.TestCase;


public class LineIteratorTest extends TestCase {

	private static final String TEXT = "0\n1\n2\n3";
	private static final CharSequence[] LINES = TEXT.split( "\n" );
	public void testLineIteratorProgressMeter() {
		testLineIterator( new ProgressLogger() );
	}

	public void testLineIterator() {
		testLineIterator( null );
	}

	public void testLineIterator( ProgressLogger pl ) {
		final LineIterator lineIterator = new LineIterator( new FastBufferedReader( new StringReader( TEXT ) ), pl );
		int i = 0;
		while( lineIterator.hasNext() )
			assertEquals( LINES[ i++ ].toString(), lineIterator.next().toString() );

		assertEquals( i, LINES.length );
	}
	
}
