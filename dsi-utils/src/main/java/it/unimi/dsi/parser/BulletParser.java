package it.unimi.dsi.parser;

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


import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.TextPattern;
import it.unimi.dsi.parser.callback.Callback;


/** A fast, lightweight, on-demand (X)HTML parser.
 * 
 * <p>The bullet parser has been written with two specific goals in mind:
 * web crawling and targeted data extraction from massive web data sets. 
 * To be usable in such environments, a parser must obey a number of 
 * restrictions:
 * <ul>
 * <li>it should avoid excessive object creation (which, for instance,
 * forbids a significant usage of Java strings);
 * <li>it should tolerate invalid syntax and recover reasonably; in fact,
 * it should never throw exceptions;
 * <li>it should perform actual parsing only on a settable feature subset:
 * there is no reason to parse the attributes of a <samp>P</samp>
 * element while searching for links;
 * <li>it should parse HTML as a <em>regular language</em>, and leave context-free
 * properties (e.g., stack maintenance and repair) to suitably designed callbacks.
 * </ul>
 * 
 * <p>Thus, in fact the bullet parser is not a parser. It is a bunch of
 * spaghetti code that analyses a stream of characters pretending that
 * it is an (X)HTML document. It has a very defensive attitude against
 * the stream character it is parsing, but at the same time it is
 * forgiving with all typical (X)HTML mistakes.
 * 
 * <p>The bullet parser is officially StringFree&trade;. 
 * <a href="http://dsiutils.dsi.unimi.it/docs/it/unimi/dsi/lang/MutableString.html"><code>MutableString</code>s</a>
 * are used for internal processing, and Java strings are used only to return attribute
 * values. All internal maps are {@linkplain it.unimi.dsi.fastutil.objects.Reference2ObjectMap reference-based maps}
 * from <a href="http://fastutil.dsi.unimi.it/"><samp>fastutil</samp></a>, which
 * helps to accelerate further the parsing process.
 * 
 * <h2>HTML data</h2>
 * 
 * <p>The bullet parser uses attributes and methods of {@link it.unimi.dsi.parser.HTMLFactory},
 * {@link it.unimi.dsi.parser.Element}, {@link it.unimi.dsi.parser.Attribute}
 * and {@link it.unimi.dsi.parser.Entity}.
 * Thus, for instance, whenever an element is to be passed around it is one
 * of the shared objects contained in {@link it.unimi.dsi.parser.Element}
 * (e.g., {@link it.unimi.dsi.parser.Element#BODY}).
 * 
 * <h2>Callbacks</h2>
 * 
 * <p>The result of the parsing process is the invocation of a callback.
 * The {@linkplain it.unimi.dsi.parser.callback.Callback callback interface}
 * of the bullet parser remembers closely SAX2, but it has some additional
 * methods targeted at (X)HTML, such as {@link it.unimi.dsi.parser.callback.Callback#cdata(it.unimi.dsi.parser.Element,char[],int,int)},
 * which returns characters found in a CDATA section (e.g., a stylesheet).
 * 
 * <p>Each callback must configure the parser, by requesting to perform
 * the analysis and the callbacks it requires. A callback that wants to
 * extract and tokenise text, for instance, will certainly require
 * {@link #parseText(boolean) parseText(true)}, but not {@link #parseTags(boolean) parseTags(true)}.
 * On the other hand, a callback wishing to extract links will require
 * to {@linkplain #parseAttribute(Attribute) parse selectively} certain attribute types.
 * 
 * <p>A more precise description follows.
 * 
 * <h2>Writing callbacks</h2>
 * 
 * <p>The first important issue is what has to be required to the parser. A newly
 * created parser does not invoke any callback. It is up to every callback
 * to add features so that it can do its job. Remember that since many
 * callbacks can be {@linkplain it.unimi.dsi.parser.callback.ComposedCallbackBuilder composed},
 * you must always <em>add</em> features, never <em>remove</em> them, and moreover
 * your callbacks must be ready to be invoked with features they did not
 * request (e.g., attribute types added by another callback).
 * 
 * <p>The following parse features
 * may be configured; most of them are just boolean features, a.k.a. flags:
 * unless otherwise specified, by default all flags are set to false (e.g., by
 * the default the parser will <em>not</em> parse tags):
 * <ul>
 * <li><em>tags</em> ({@link #parseTags(boolean)} method): whether tags
 * should be parsed;
 * <li><em>attributes</em> ({@link #parseAttributes(boolean)} and
 * {@link #parseAttribute(Attribute) methods)}:
 * whether attributes should be parsed (of course, setting this flag is useless
 * if you are not parsing tags); note that setting this flag will just
 * activate the attribute parsing feature, but you must also
 * {@linkplain #parseAttribute(Attribute) register} every attribute 
 * whose value you want to obtain.
 * <li><em>text</em> ({@link #parseText(boolean)}method): whether text
 * should be parsed; if this flag is set, the parser will call the
 * {@link it.unimi.dsi.parser.callback.Callback#characters(char[], int, int, boolean)}
 * method for every text chunk found.
 * <li><em>CDATA sections</em> ({@link #parseCDATA(boolean)}method): whether CDATA
 * sections (stylesheets &amp; scripts)
 * should be parsed; if this flag is set, the parser will call the
 * {@link it.unimi.dsi.parser.callback.Callback#cdata(Element,char[],int,int)}
 * method for every CDATA section found.
 * </ul>
 * 
 * <h2>Invoking the parser</h2>
 * 
 * <p>After {@linkplain #setCallback(Callback) setting the parser callback}, 
 * you just call {@link #parse(char[], int, int)}.
 */

public class BulletParser {

	private static final boolean DEBUG = false;

	/** Scanning text.. */
	protected static final int STATE_TEXT = 0;
	/** Scanning attribute name/value pairs. */
	protected static final int STATE_BEFORE_START_TAG_NAME = 1;
	/** Scanning a closing tag. */
	protected static final int STATE_BEFORE_END_TAG_NAME = 2;
	/** Scanning attribute name/value pairs. */
	protected static final int STATE_IN_START_TAG = 3;
	/** Scanning a closing tag. */
	protected static final int STATE_IN_END_TAG = 4;

	/** The maximum Unicode value accepted for a numeric entity. */
	protected static final int MAX_ENTITY_VALUE = 65535;
	/** The base for non-decimal entity. */
	protected static final int HEXADECIMAL = 16;
	/** The maximum number of digits of a hexadecimal numeric entity. */
	protected static final int MAX_HEX_ENTITY_LENGTH = 8;
	/** The maximum number of digits of a decimal numeric entity. */
	protected static final int MAX_DEC_ENTITY_LENGTH = 9;

	/** Closing tag for a script element. */
	protected static final TextPattern SCRIPT_CLOSE_TAG_PATTERN = new TextPattern( "</script>", TextPattern.CASE_INSENSITIVE );
	/** Closing tag for a style element. */
	protected static final TextPattern STYLE_CLOSE_TAG_PATTERN = new TextPattern( "</style>", TextPattern.CASE_INSENSITIVE );

	/** An array containing the non-space whitespace. */
	protected static final char[] NONSPACE_WHITESPACE = { '\n', '\r', '\t' };
	/** An array, parallel to {@link #NONSPACE_WHITESPACE}, containing spaces. */
	protected static final char[] SPACE = { ' ', ' ', ' ' };
	
	/** Closed comment. It should be "-->", but mistakes are common. */
	protected static final TextPattern CLOSED_COMMENT = new TextPattern( "->" );
	/** Closed ASP or similar tag. */
	protected static final TextPattern CLOSED_PERCENT = new TextPattern( "%>" );
	/** Closed processing instruction. */
	protected static final TextPattern CLOSED_PIC = new TextPattern( "?>" );
	/** Closed section (conditional, etc.). */
	protected static final TextPattern CLOSED_SECTION = new TextPattern( "]>" );
	/** Closed section (conditional, CDATA, etc.). */
	protected static final TextPattern CLOSED_CDATA = new TextPattern( "]]>" );
	/** TODO: what is this?. */
	//protected static final TextPattern CLOSED_BOH = new TextPattern( "!>" );

	/** The parsing factory used by this parser. */
	public final ParsingFactory factory;
	
	/** The callback of this parser. */
	protected Callback callback;
	/** A map from attributes to attribute values. */
	protected Reference2ObjectMap<Attribute,MutableString> attrMap;
	/** Whether we should invoke the text handler. */
	protected boolean parseText;
	/** Whether we should invoke the CDATA section handler. */
	protected boolean parseCDATA;
	/** Whether we should parse tags. */
	protected boolean parseTags;
	/** Whether we should parse attributes. */
	protected boolean parseAttributes;
	/**
	 * The subset of attributes whose values will be actually parsed (if, of
	 * course, {@link #parseAttributes}is true).
	 */
	protected ReferenceArraySet<Attribute> parsedAttrs = new ReferenceArraySet<Attribute>();
	/**
	 * An externally visible, immutable subset of attributes whose values will
	 * be actually parsed.
	 */
	public ReferenceSet<Attribute> parsedAttributes = ReferenceSets.unmodifiable( parsedAttrs );
	/** The character represented by the last scanned entity. */
	protected char lastEntity;
	
	/** Creates a new bullet parser. */
	public BulletParser( final ParsingFactory factory ) {
		this.factory = factory;
	}

	/** Creates a new bullet parser using the default factory {@link HTMLFactory#INSTANCE}. */
	public BulletParser() {
		this( HTMLFactory.INSTANCE );
	}

	/**
	 * Returns whether this parser will invoke the text handler.
	 * 
	 * @return whether this parser will invoke the text handler.
	 * @see #parseText(boolean)
	 */
	public boolean parseText() {
		return parseText;
	}

	/**
	 * Sets the text handler flag.
	 * 
	 * @param parseText
	 *            the new value.
	 * @return this parser.
	 */
	public BulletParser parseText( final boolean parseText ) {
		this.parseText = parseText;
		return this;
	}

	/**
	 * Returns whether this parser will invoke the CDATA-section handler.
	 * 
	 * @return whether this parser will invoke the CDATA-section handler.
	 * @see #parseCDATA(boolean)
	 */
	public boolean parseCDATA() {
		return parseCDATA;
	}

	/**
	 * Sets the CDATA-section handler flag.
	 * 
	 * @param parseCDATA
	 *            the new value.
	 * @return this parser.
	 */
	public BulletParser parseCDATA( final boolean parseCDATA ) {
		this.parseCDATA = parseCDATA;
		return this;
	}

	/**
	 * Returns whether this parser will parse tags and invoke element handlers.
	 * 
	 * @return whether this parser will parse tags and invoke element handlers.
	 * @see #parseTags(boolean)
	 */
	public boolean parseTags() {
		return parseTags;
	}

	/**
	 * Sets whether this parser will parse tags and invoke element handlers.
	 * 
	 * @param parseTags
	 *            the new value.
	 * @return this parser.
	 */
	public BulletParser parseTags( final boolean parseTags ) {
		this.parseTags = parseTags;
		return this;
	}

	/**
	 * Returns whether this parser will parse attributes.
	 * 
	 * @return whether this parser will parse attributes.
	 * @see #parseAttributes(boolean)
	 */
	public boolean parseAttributes() {
		return parseAttributes;
	}

	/**
	 * Sets the attribute parsing flag.
	 * 
	 * @param parseAttributes
	 *            the new value for the flag.
	 * @return this parser.
	 */
	public BulletParser parseAttributes( final boolean parseAttributes ) {
		this.parseAttributes = parseAttributes;
		return this;
	}

	/**
	 * Adds the given attribute to the set of attributes to be parsed.
	 * 
	 * @param attribute
	 *            an attribute that should be parsed.
	 * @throws IllegalStateException
	 *             if {@link #parseAttributes(boolean) parseAttributes(true)}
	 *             has not been invoked on this parser.
	 * @return this parser.
	 */
	public BulletParser parseAttribute( final Attribute attribute ) {
		parsedAttrs.add( attribute );
		return this;
	}

	/** Sets the callback for this parser, resetting at the same time all parsing flags.
	 * 
	 * @param callback the new callback.
	 * @return this parser.
	 */
	public BulletParser setCallback( final Callback callback ) {
        this.callback = callback;
        parseCDATA = parseText = parseAttributes = parseTags = false;
        parsedAttrs.clear();
        callback.configure( this );
        return this;
	}

	/** Returns the character corresponding to a given entity name.
	 *
	 * @param name the name of an entity.
	 * @return the character corresponding to the entity, or an ASCII NUL if no entity with that name was found.
	 */
	protected char entity2Char( final MutableString name ) {
		final Entity e = factory.getEntity( name );
		return e == null ? (char)0 : e.character;
	}

	/** Searches for the end of an entity.
	 * 
	 * <P>This method will search for the end of an entity starting at the given offset (the offset
	 * must correspond to the ampersand).
	 * 
	 * <P>Real-world HTML pages often contain hundreds of misplaced ampersands, due to the
	 * unfortunate idea of using the ampersand as query separator (<em>please</em> use the comma
	 * in new code!). All such ampersand should be specified as <samp>&amp;amp;</samp>. 
	 * If named entities are delimited using a transition
	 * from alphabetical to non-alphabetical characters, we can easily get false positives. If the parameter
	 * <code>loose</code> is false, named entities can be delimited only by whitespace or by a comma.
	 * 
	 * @param a a character array containing the entity.
	 * @param offset the offset at which the entity starts (the offset must point at the ampersand).
	 * @param length an upper bound to the maximum returned position.
	 * @param loose if true, named entities can be terminated by any non-alphabetical character 
	 * (instead of whitespace or comma).
	 * @param entity a support mutable string used to query {@link ParsingFactory#getEntity(MutableString)}.
	 * @return the position of the last character of the entity, or -1 if no entity was found.
	 */
	protected int scanEntity( final char[] a, final int offset, final int length, final boolean loose, final MutableString entity ) {

		int i, c = 0;
		String tmpEntity;

		if ( length < 2 ) return -1;
		
		if ( a[ offset + 1 ] == '#' ) {
			if ( length > 2 && a[ offset + 2 ] == 'x' ) {
				for( i = 3; i < length && i < MAX_HEX_ENTITY_LENGTH && Character.digit( a[ i + offset ], HEXADECIMAL ) != -1; i++ );
				tmpEntity =  new String( a, offset + 3, i - 3 );
				if ( i != 3 ) c = Integer.parseInt( tmpEntity, HEXADECIMAL );
			}
			else {
				for( i = 2; i < length && i < MAX_DEC_ENTITY_LENGTH && Character.isDigit( a[ i + offset ] ); i++ );
				tmpEntity = new String( a, offset + 2, i - 2 );
				if ( i != 2 ) c = Integer.parseInt( tmpEntity );
			}
			
			if ( c > 0 && c < MAX_ENTITY_VALUE ) {
				lastEntity = (char)c;
				if ( i < length && a[ i + offset ] == ';' ) i++;
				return i + offset;
			}
		}
		else {
			if ( Character.isLetter( a[ offset + 1 ] ) ) {
				for( i = 2; i < length && Character.isLetterOrDigit( a[ offset + i ] ); i++ );
				if ( i != 1 && ( loose || ( i < length && ( Character.isWhitespace( a[ offset + i ] ) || a[ offset + i ] == ';' ) ) ) && ( lastEntity = entity2Char( entity.length( 0 ).append( a, offset + 1, i - 1 ) ) ) != 0 ) {
					if ( i < length && a[ i + offset ] == ';' ) i++;
					return i + offset;
				}
			}
		}

		return -1;
	}

	/**
	 * Replaces entities with the corresponding characters.
	 * 
	 * <P>This method will modify the mutable string <code>s</code> so that all legal occurrences
	 * of entities are replaced by the corresponding character.
	 * 
	 * @param s a mutable string whose entities will be replaced by the corresponding characters.
	 * @param entity a support mutable string used by {@link #scanEntity(char[], int, int, boolean, MutableString)}.
	 * @param loose a parameter that will be passed to {@link #scanEntity(char[], int, int, boolean, MutableString)}.
	 */
	protected void replaceEntities( final MutableString s, final MutableString entity, final boolean loose ) {

		final char[] a = s.array();
		int length = s.length();

		/* We examine the string *backwards*, so that i is always a valid index. */

		int i = length, j;
		while( i-- > 0 )
			if ( a[ i ] == '&' && ( j = scanEntity( a, i, length - i, loose, entity ) ) != -1 ) 
				length = s.replace( i, j, lastEntity ).length();
	}

	/** Handles markup.
	 * 
	 * @param text the text.
	 * @param pos the first character in the markup after <samp>&lt;!</samp>.
	 * @param end the end of <code>text</code>.
	 * @return the position of the first character after the markup.
	 */
	
	protected int handleMarkup( final char[] text, int pos, final int end ) {
		// A markup instruction (doctype, comment, etc.).
		switch( text[ ++pos ] ) {
		case 'D':
		case 'd':
			// DOCTYPE
			while(  pos < end && text[ pos++ ] != '>' );
			break;

		case '-':
			// comment
			if ( ( pos = CLOSED_COMMENT.search( text, pos, end ) ) == -1 ) pos = end;
			else pos += CLOSED_COMMENT.length();
			break;
		
		default:
			if ( pos < end - 6 && 
					text[ pos ] == '[' && text[ pos + 1 ] == 'C' && text[ pos + 2 ] == 'D' && text[ pos + 3 ] == 'A' && text[ pos + 4 ] == 'T' && text[ pos + 5 ] == 'A' && text[ pos + 6 ] == '[' ) {
				// CDATA section
				final int last = CLOSED_CDATA.search( text, pos, end );
				if ( parseCDATA ) callback.cdata( null, text, pos + 7, ( last == -1 ? end : last ) - pos - 7 );
				pos = last == -1 ? end : last + CLOSED_CDATA.length();
			}
			//  Generic markup
			else while( pos < end && text[ pos++ ] != '>' );
			break;
		}

		return pos;
	}
	
	/** Handles processing instruction, ASP tags etc.
	 * 
	 * @param text the text.
	 * @param pos the first character in the markup after <samp>&lt;%</samp>.
	 * @param end the end of <code>text</code>.
	 * @return the position of the first character after the processing instruction.
	 */
	
	protected int handleProcessingInstruction( final char[] text, int pos, final int end ) {

		switch( text[ ++pos  ] ) {
		case '%':
			if ( ( pos = CLOSED_PERCENT.search( text, pos, end ) ) == -1 ) pos = end;
			else pos += CLOSED_PERCENT.length();
			break;
			
		case '?':
			if ( ( pos = CLOSED_PIC.search( text, pos, end ) ) == -1 ) pos = end;
			else pos += CLOSED_PIC.length();
			break;
		case '[':
			if ( ( pos = CLOSED_SECTION.search( text, pos, end ) ) == -1 ) pos = end;
			else pos += CLOSED_SECTION.length();
			break;
		default:
			//  Generic markup
			while( pos < end && text[ pos++ ] != '>' );
			break;
		}
		return pos;
	}

	
	/**
	 * Analyze the text document to extract information.
	 * 
	 * @param text a <code>char</code> array of text to be parsed.
	 */
	public void parse( final char[] text ) {
		parse( text, 0, text.length );
	}
		
	/**
	 * Analyze the text document to extract information.
	 * 
	 * @param text a <code>char</code> array of text to be parsed.
	 * @param offset the offset in the array from which the parsing will begin.
	 * @param length the number of characters to be parsed.
	 */
	public void parse( final char[] text, final int offset, final int length ) {
		MutableString tagElemTypeName = new MutableString(); 
		MutableString attrName = new MutableString(); 
		MutableString attrValue = new MutableString(); 
		MutableString entity = new MutableString();
		MutableString characters = new MutableString();

		/* During the analysis of attribute we need a separator for values */
		char delim;
		/* The current character */
		char currChar;
		/* The state of the switch */
		int state;
		/* Others integer values used in the parsing process */
		int start, k;
		/* This boolean is set true if we have words to handle */
		boolean flowBroken = false, parseCurrAttr;
		
		/* The current element. */
		Element currentElement;
		/* The current attribute object */
		Attribute currAttr = null; 
		attrMap = new Reference2ObjectArrayMap<Attribute,MutableString>( 16 );
	
		callback.startDocument();

		tagElemTypeName.length( 0 ); 
		attrName.length( 0 ); 
		attrValue.length( 0 ); 
		entity.length( 0 ); 

		state = STATE_TEXT;
		currentElement = null;
		final int end = offset + length;
		int pos = offset;
		
		/* This is the main loop. */
		while ( pos < end ) {
			
			switch( state ) {
			case STATE_TEXT:
				currChar = text[ pos ];
				if ( currChar == '&' ) {
					
					// We handle both the case of an entity, and that of a stray '&'.
					if ( ( k = scanEntity( text, pos, end - pos, true, entity ) ) == -1 ) {
						currChar = '&';
						pos++;
					}
					else {
						currChar = lastEntity;
						pos = k;
						if ( DEBUG ) System.err.println( "Entity at: " + pos + " end of entity: " + k + " entity: " + entity + " char: " + currChar );
					}
					if ( parseText ) characters.append( currChar );
					continue;
				}
				
				// No tags can happen later than end - 2.
				if ( currChar != '<' || pos >= end - 2 ) {
					if ( parseText ) characters.append( currChar );
					pos++;
					continue;
				}
				
				switch( text[ ++pos ] ) {
				case '!':
					pos = handleMarkup( text, pos, end );
					break;

				case '%':
				case '?':
					pos = handleProcessingInstruction( text, pos, end );
					break;

				default:
					// Actually a tag. Note that we allow for </> and that we skip false positives
					// due to sloppy HTML writing (e.g., "<-- hello! -->" ).
					if ( Character.isLetter( text[ pos ] ) ) state = STATE_BEFORE_START_TAG_NAME;
					else if ( text[ pos ] == '/' && ( Character.isLetter( text[ pos + 1 ] ) || text[ pos + 1 ] == '>' ) ) {
						state = STATE_BEFORE_END_TAG_NAME;
						pos++;
					}
					else {
						// Not really a tag.
						if ( parseText ) characters.append( '<' );
						continue;
					}
					break;
				}
				if ( parseText && characters.length() != 0 ) {
					callback.characters( characters.array(), 0, characters.length(), flowBroken );
					characters.length( 0 );
				}

				flowBroken = false;				
				break;

			case STATE_BEFORE_START_TAG_NAME:
			case STATE_BEFORE_END_TAG_NAME:
				// Let's get the name.
				tagElemTypeName.length( 0 );
				for( start = pos; pos < end && ( Character.isLetterOrDigit( text[ pos ] ) || text[ pos ] == ':' || text[ pos ] == '_' ||text[ pos ] == '-' || text[ pos ] == '.' ); pos++ );
				
				tagElemTypeName.append( text, start, pos - start );
				tagElemTypeName.toLowerCase();
				
				currentElement = factory.getElement( tagElemTypeName );
				if ( DEBUG ) System.err.println( ( state == STATE_BEFORE_START_TAG_NAME ? "Opening" : "Closing" ) + " tag for " + tagElemTypeName + " (element: " + currentElement+ ")" );
				
				if ( currentElement != null && currentElement.breaksFlow ) flowBroken = true;
				while( pos < end && Character.isWhitespace( text[ pos ] ) ) pos++;
				state = state == STATE_BEFORE_START_TAG_NAME ? STATE_IN_START_TAG : STATE_IN_END_TAG;
				break;
				
			case STATE_IN_START_TAG:
				currChar = text[ pos ];
				if ( currChar != '>' && ( currChar != '/' || pos == end - 1 || text[ pos + 1 ] != '>' ) ) {
					// We got attributes.
					if ( Character.isLetter( currChar ) ) {
						parseCurrAttr = false;
						attrName.length( 0 );
						for( start = pos; pos < end && ( Character.isLetter( text[ pos ] ) || text[ pos ] == '-' ); pos++ );
						if ( currentElement != null && parseAttributes ) { 
							attrName.append( text, start, pos - start );
							attrName.toLowerCase();
							if ( DEBUG ) System.err.println( "Got attribute named \"" + attrName + "\"" );
							currAttr = factory.getAttribute( attrName );
							parseCurrAttr = parsedAttrs.contains( currAttr );
						}
						// Skip whitespace
						while ( pos < end && Character.isWhitespace( text[ pos ] ) ) pos++;
						if ( pos == end ) break;
						if ( text[ pos ] != '=' ) {
							// We found an attribute without explicit value.
							// TODO: can we avoid another string?
							if ( parseCurrAttr ) attrMap.put( currAttr, new MutableString( currAttr.name ) );
							break;
						}
						
						pos++;
						while ( pos < end && Character.isWhitespace( text[ pos ] ) ) pos++;
						if ( pos == end ) break;
						
						attrValue.length( 0 );
						if ( pos < end && ( ( delim = text[ pos ] ) == '"' || ( delim = text[ pos ] ) == '\'' ) ) {
							// An attribute value with delimiters.
							for( start = ++pos; pos < end && text[ pos ] != delim; pos++ );
							if ( parseCurrAttr ) attrValue.append( text, start, pos - start ).replace( NONSPACE_WHITESPACE, SPACE );
							if ( pos < end ) pos++;
						}
						else {
							// An attribute value without delimiters. Due to very common errors, we 
							// gather characters up to the first occurrence of whitespace or '>'.
							for( start = pos; pos < end && !Character.isWhitespace( text[ pos ] ) && text[ pos ] != '>'; pos++ ); 
							if ( parseCurrAttr ) attrValue.append( text, start, pos - start );
						}

						if ( parseCurrAttr ) {
							replaceEntities( attrValue, entity, false );
							attrMap.put( currAttr, attrValue.copy() );
							if ( DEBUG ) System.err.println( "Attribute value: \"" + attrValue + "\"" );
						}
						// Skip whitespace
						while ( pos < end && Character.isWhitespace( text[ pos ] ) ) pos++;
					}
					else {
						// It's a mess. Our only reasonable chance is to try to resync on the first
						// whitespace, or alternatively to get to the end of the tag.
						do pos++; while ( pos < end && text[ pos ] != '>' && ! Character.isWhitespace( text[ pos ] ) );
						// Skip whitespace
						while ( pos < end && Character.isWhitespace( text[ pos ] ) ) pos++;
						continue;
					}
				}
				else {
					if ( parseTags && ! callback.startElement( currentElement, attrMap ) ) break;
					if ( attrMap != null ) attrMap.clear();
					
					if ( currentElement == Element.SCRIPT || currentElement == Element.STYLE ) {
						final TextPattern pattern = currentElement == Element.SCRIPT ? SCRIPT_CLOSE_TAG_PATTERN : STYLE_CLOSE_TAG_PATTERN; 
						start = pos + 1;
						pos = pattern.search( text, start, end );
						if ( pos == -1 ) pos = end;
						if ( parseText ) callback.cdata( currentElement, text, start, pos - start );
						if ( pos < end ) {
							if ( parseTags ) callback.endElement( currentElement );
							pos += pattern.length();
						}
					}
					else pos += currChar == '/' ? 2 : 1;
					state = STATE_TEXT;
				}
				break;
				
			case STATE_IN_END_TAG:
				while ( pos < end && text[ pos ] != '>' ) pos++;
				if ( parseTags && currentElement != null && ! callback.endElement( currentElement ) ) break;
				state = STATE_TEXT;
				pos++;
				break;
				
			default:
			}
			
		}

		// We do what we can to invoke tag handlers in case of a truncated text.
		if ( state == STATE_IN_START_TAG && parseTags && currentElement != null ) callback.startElement( currentElement, attrMap );
		if ( state == STATE_IN_END_TAG && parseTags && currentElement != null ) callback.endElement( currentElement );
		
		if ( state == STATE_TEXT && parseText && characters.length() > 0 ) 
			callback.characters( characters.array(), 0, characters.length(), flowBroken );
		
		callback.endDocument();
	}
}
