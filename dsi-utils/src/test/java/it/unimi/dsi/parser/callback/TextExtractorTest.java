package it.unimi.dsi.parser.callback;

import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.callback.TextExtractor;
import junit.framework.TestCase;

public class TextExtractorTest extends TestCase {

	public void testBRBreaksFlow() {
		char a[] = "ciao<BR>mamma<BR>".toCharArray();
		BulletParser bulletParser = new BulletParser();
		TextExtractor textExtractor = new TextExtractor();
		bulletParser.setCallback( textExtractor );
		bulletParser.parse( a );
		assertTrue( textExtractor.text.toString(), textExtractor.text.indexOf( ' ' ) != -1 );
	}

}
