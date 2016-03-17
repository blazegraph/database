package it.unimi.dsi.parser.callback;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;
import it.unimi.dsi.util.TextPattern;

import java.util.Map;
import java.util.Set;

/**
 * A callback extracting links.
 * 
 * <P>This callbacks extracts links existing in the web page. The
 * links are then accessible in {@link #urls} (a set of {@link String}s). Note that
 * we guarantee that the iteration order in the set is exactly
 * the order in which links have been met (albeit copies appear
 * just once).
 */


public class LinkExtractor extends DefaultCallback {
	/** The pattern prefixing the URL in a <samp>META </samp> <samp>HTTP-EQUIV </samp> element of refresh type. */
	private static final TextPattern URLEQUAL_PATTERN = new TextPattern( "URL=", TextPattern.CASE_INSENSITIVE );

	/** The URLs resulting from the parsing process. */
	public final Set<String> urls = new ObjectLinkedOpenHashSet<String>();

	/** The URL contained in the first <samp>META </samp> <samp>HTTP-EQUIV </samp> element of refresh type (if any). */
	private String metaRefresh = null;

	/** The URL contained in the first <samp>META </samp> <samp>HTTP-EQUIV </samp> element of location type (if any). */
	private String metaLocation = null;

	/** The URL contained in the first <samp>BASE </samp> element (if any). */
	private String base = null;


	/**
	 * Configure the parser to parse elements and certain attributes.
	 * 
	 * <p>
	 * The required attributes are <samp>SRC </samp>, <samp>HREF </samp>, <samp>HTTP-EQUIV </samp>, and <samp>CONTENT
	 * </samp>.
	 *  
	 */

	public void configure( final BulletParser parser ) {
		parser.parseTags( true );
		parser.parseAttributes( true );
		parser.parseAttribute( Attribute.SRC );
		parser.parseAttribute( Attribute.HREF );
		parser.parseAttribute( Attribute.HTTP_EQUIV );
		parser.parseAttribute( Attribute.CONTENT );
	}

	public void startDocument() {
		urls.clear();
		base = metaLocation = metaRefresh = null;
	}

	public boolean startElement( final Element element, final Map<Attribute,MutableString> attrMap ) {
		Object s;

		// TODO: what about IMG?
		
		if ( element == Element.A || element == Element.AREA || element == Element.LINK ) {
			s = attrMap.get( Attribute.HREF );
			if ( s != null )
				urls.add( s.toString() );
		}

		// IFRAME or FRAME + SRC
		if ( element == Element.IFRAME || element == Element.FRAME || element == Element.EMBED ) {
			s = attrMap.get( Attribute.SRC );
			if ( s != null )
				urls.add( s.toString() );
		}

		// BASE + HREF (change context!)
		if ( element == Element.BASE && base == null ) {
			s = attrMap.get( Attribute.HREF );
			if ( s != null )
				base = s.toString();
		}

		// META REFRESH/LOCATION
		if ( element == Element.META ) {
			final MutableString equiv = attrMap.get( Attribute.HTTP_EQUIV );
			final MutableString content = attrMap.get( Attribute.CONTENT );
			if ( equiv != null && content != null ) {
				equiv.toLowerCase();

				// http-equiv="refresh" content="0;URL=http://foo.bar/..."
				if ( equiv.equals( "refresh" ) && ( metaRefresh == null ) ) {

					final int pos = URLEQUAL_PATTERN.search( content );
					if ( pos != -1 )
						metaRefresh = content.substring( pos + URLEQUAL_PATTERN.length() ).toString();
				}

				// http-equiv="location" content="http://foo.bar/..."
				if ( equiv.equals( "location" ) && ( metaLocation == null ) )
					metaLocation = attrMap.get( Attribute.CONTENT ).toString();
			}
		}

		return true;
	}

	/**
	 * Returns the URL specified by <samp>META </samp> <samp>HTTP-EQUIV </samp> elements of location type. More
	 * precisely, this method returns a non- <code>null</code> result iff there is at least one <samp>META HTTP-EQUIV
	 * </samp> element specifying a location URL (if there is more than one, we keep the first one).
	 * 
	 * @return the first URL specified by a <samp>META </samp> <samp>HTTP-EQUIV </samp> elements of location type, or
	 *         <code>null</code>.
	 */
	public String metaLocation() {
		return metaLocation;
	}

	/**
	 * Returns the URL specified by the <samp>BASE </samp> element. More precisely, this method returns a non-
	 * <code>null</code> result iff there is at least one <samp>BASE </samp> element specifying a derelativisation URL
	 * (if there is more than one, we keep the first one).
	 * 
	 * @return the first URL specified by a <samp>BASE </samp> element, or <code>null</code>.
	 */
	public String base() {
		return base;
	}

	/**
	 * Returns the URL specified by <samp>META </samp> <samp>HTTP-EQUIV </samp> elements of refresh type. More
	 * precisely, this method returns a non- <code>null</code> result iff there is at least one <samp>META HTTP-EQUIV
	 * </samp> element specifying a refresh URL (if there is more than one, we keep the first one).
	 * 
	 * @return the first URL specified by a <samp>META </samp> <samp>HTTP-EQUIV </samp> elements of refresh type, or
	 *         <code>null</code>.
	 */
	public String metaRefresh() {
		return metaRefresh;
	}
}
