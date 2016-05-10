
package it.unimi.dsi.parser.callback;

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.callback.LinkExtractor;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

public class LinkExtractorTest extends TestCase {

	public void testExtractor() throws IOException {
//		char[] text = IOUtils.toCharArray( this.getClass().getResourceAsStream( "LinkExtractorTest1.html" ), "UTF-8" );
//
//		BulletParser parser = new BulletParser();
//		LinkExtractor linkExtractor = new LinkExtractor();
//		parser.setCallback( linkExtractor );
//		parser.parse( text );
//
//		testExtractorResults( linkExtractor );
//		Test resource not included in 1.10.0 source distribution
	  	assertTrue(true);
	}

	private void testExtractorResults( final LinkExtractor linkExtractor ) {
		assertEquals( new ObjectLinkedOpenHashSet<String>( new String[] { "manual.css", "http://link.com/", "http://anchor.com/", "http://badanchor.com/" } ), linkExtractor.urls );
		assertEquals( "http://base.com/", linkExtractor.base() );
		assertEquals( "http://refresh.com/", linkExtractor.metaRefresh() );
		assertEquals( "http://location.com/", linkExtractor.metaLocation() );
	}
}
