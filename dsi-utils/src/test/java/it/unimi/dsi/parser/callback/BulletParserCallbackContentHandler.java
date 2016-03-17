package it.unimi.dsi.parser.callback;

import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;
import it.unimi.dsi.parser.callback.Callback;

import java.util.Iterator;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/** An adapter from callbacks of the bullet parser to standard
 * SAX content handler. Can be used to run, eg., <samp>tagsoup</samp>
 * and the bullet parser against a page and check that the
 * actual callback invocations are the same.
 */

public class BulletParserCallbackContentHandler extends DefaultHandler {
	/** The delegated callback. */
	private final Callback callback;

	/** The corresponding parser. */
	private final BulletParser parser;

	/** The element enclosing the current CDATA section, or <code>null</code> 
	 * if we're not in a CDATA section. */
	private Element inCdata;
	
	/** The set of tags enclosing CDATA sections. */
	private final ReferenceSet<Element> cdataElements = new ReferenceOpenHashSet<Element>( new Element[] { Element.SCRIPT, Element.STYLE } );
	
	/** The map used to fake an attribute map. */
	private final Reference2ObjectOpenHashMap<Attribute,MutableString> attrMap = new Reference2ObjectOpenHashMap<Attribute, MutableString>(  );

	public BulletParserCallbackContentHandler( final BulletParser parser, final Callback callback ) {
		this.parser = parser;
		this.callback = callback;
	}

	public void endDocument() {
		callback.endDocument();
	}

	public void endElement( final String uri, final String localName, final String qName ) {
	    Element element = parser.factory.getElement( new MutableString( localName ) );
		if ( cdataElements.contains( element ) && element == inCdata ) inCdata = null;
		callback.endElement( element );
	}

	public void characters( final char[] ch, final int start, final int length ) {
		if ( inCdata != null ) callback.cdata( inCdata, ch, start, length );
		else callback.characters( ch, start, length, false );
	}

	public void ignorableWhitespace( final char[] ch, final int start, final int length ) {
		callback.characters( ch, start, length, false );
	}
	
	public void startDocument() {
		callback.startDocument();
	}

	public void startElement( final String uri, final String localName, final String qName, final Attributes attributes ) {
		attrMap.clear(  );
		String value;
		Attribute attribute;

		for ( Iterator<Attribute> i = parser.parsedAttributes.iterator(); i.hasNext();) {
			attribute = i.next();
			value = attributes.getValue( attribute.toString(  ) );
			if ( value != null )
				attrMap.put( attribute, new MutableString( value ) );
		}
		
		Element element = parser.factory.getElement( new MutableString( localName ) );
		if ( cdataElements.contains( element ) ) inCdata = element;
		callback.startElement( element, attrMap );
	}
}
