package it.unimi.dsi.io;

import it.unimi.dsi.io.FastBufferedReader;
import junit.framework.TestCase;

public class FastBufferedReaderTest extends TestCase {

	public void testToSpec() {
		String className = FastBufferedReader.class.getName();
		assertEquals( className, new FastBufferedReader().toSpec() );
		assertEquals( className + "(100)", new FastBufferedReader( 100 ).toSpec() );
		assertEquals( className + "(\"_\")", new FastBufferedReader( "_" ).toSpec() );
		assertEquals( className + "(100,\"_\")", new FastBufferedReader( "100", "_" ).toSpec() );
	}
}
