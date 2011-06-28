/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Mar 9, 2011
 */

package com.bigdata.util.httpd;

import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

/**
 * This class provides set of utilities for encoding and decoding HTTP
 * headers and doubles as the base class for all classes that
 * implement support for a specific HTTP header, such as {@link
 * LinkHeader}, {@link AcceptHeader}, etc.
 */

public class HTTPHeaderUtility
{

    /**
     * The {@link Logger} for HTTP header operations, including
     * parsing and serialization.  The {@link Logger} is named for
     * this class.  It should be used by all derived classes.
     */

    protected static final Logger log = Logger.getLogger
	( HTTPHeaderUtility.class
	  );

    // quoted-string
    //           :=
    //
    // token     := One of more of any CHAR except CTLs or separators.
    //
    // CTL       := <any US-ASCII control character (octets 0 - 31) and
    //               DEL (127)>
    //
    // separators := "(" | ")" | "<" | ">" | "@"
    //             | "," | ";" | ":" | "\" | <">
    //             | "/" | "[" | "]" | "?" | "="
    //             | "{" | "}" | SP | HT

    // token     := One of more of any CHAR except CTLs or separators.
    //
    // CTL       := <any US-ASCII control character (octets 0 - 31) and
    //               DEL (127)>
    //
    // separators := "(" | ")" | "<" | ">" | "@"
    //             | "," | ";" | ":" | "\" | <">
    //             | "/" | "[" | "]" | "?" | "="
    //             | "{" | "}" | SP | HT
    
    /**
     * Matches an HTTP <code>token</code>, which consists of one or
     * more of any CHAR except <code>CTL</code> or
     * <code>separator</code>.
     */

    final protected static String httpTokenPattern =
	"[^\\p{Cntrl}\\(\\)<>@,;:\\\\\\\"/\\[\\]\\?=\\{\\}\\s\\x09]+"
	;
    
    /**
     * The text for a {@link Pattern} matching an HTTP
     * <code>quoted-string</code>.<p>
     *
     * From HTTP/1.1:<p>
     *
     * A string of text is parsed as a single word if it is quoted
     * using double-quote marks.<p>
     *
     * The backslash character '\' MAY be used as a single-character
     * quoting mechanism only within quoted-string and comment
     * constructs.<p>
     *

<pre>

quoted-string = ( <"> *(qdtext | quoted-pair ) <"> )
qdtext = <any TEXT except <">>
quoted-pair = "\" CHAR
TEXT = <any OCTET except CTLs, but including LWS>

</pre>

     * Note: This pattern text uses a non-capturing group to
     * encapsulate the choice between a legal character (TEXT) and an
     * escaped character (quoted-pair) within the context of the
     * quoted-string. Any quoted-pair sequences must be reversed by
     * the consumer of the matched group.<p>
     */

    final protected static String httpQuotedStringPattern =
	"\\\"(?:\\\\\"|[^\\p{Cntrl}\\\"])*\\\""
	;

    /**
     * Returns true iff the character is an HTTP <code>CTL</code>
     * character.
     */

    static public boolean isHttpCtlChar( char ch )
    {

	int c = (int)ch;
	
	if( c >= 0 && c <= 31 ) return true;
	
	if( c == 127 ) return true;
	
	return false;

    }
    
    /**
     * Returns true iff the character is an HTTP
     * <code>separator</code> character.
     */

    static public boolean isHttpSeparatorChar( char ch )
    {

	switch( ch ) {
	case '(':
	case ')':
	case '<':
	case '>':
	case '@':
	case ',':
	case ';':
	case ':':
	case '\\':
	case '"':
	case '/':
	case '[':
	case ']':
	case '?':
	case '=':
	case '{':
	case '}':
	case ' ':		// ASCII space (32).
	case (char)0x09:	// Horizontal tab (9).

	    return true;

	}

	return false;

    }
    
    /**
     * Matches an HTTP <code>token</code>, which consists of one or
     * more of any CHAR except <code>CTL</code> or
     * <code>separator</code>.
     */

    final protected static String tok =
	"[^\\p{Cntrl}\\(\\)<>@,;:\\\\\\\"/\\[\\]\\?=\\{\\}\\s\\x09]+"
	;
    
    /**
     * Returns true iff the {@link String} obeys the syntax rules
     * for an HTTP <code>token</code>.
     */

    static public boolean isHttpToken( String token )
    {
	
	int len = token.length();
	
	for( int i=0; i<len; i++ ) {
	    
	    char ch = token.charAt( i );
	    
	    if( isHttpCtlChar( ch ) ||
		isHttpSeparatorChar( ch )
		) {
		
		return false;
		
	    }
	    
	}
	
	return true;
	
    }
    
    /**
     * The text for a {@link Pattern} matching an HTTP
     * <code>quoted-string</code>.<p>
     *
     * From HTTP/1.1:<p>
     *
     * A string of text is parsed as a single word if it is quoted
     * using double-quote marks.<p>
     *
     * The backslash character '\' MAY be used as a single-character
     * quoting mechanism only within quoted-string and comment
     * constructs.<p>
     *

<pre>

quoted-string = ( <"> *(qdtext | quoted-pair ) <"> )
qdtext = <any TEXT except <">>
quoted-pair = "\" CHAR
TEXT = <any OCTET except CTLs, but including LWS>

</pre>

     * Note: This pattern text uses a non-capturing group to
     * encapsulate the choice between a legal character (TEXT) and an
     * escaped character (quoted-pair) within the context of the
     * quoted-string. Any quoted-pair sequences must be reversed by
     * the consumer of the matched group.<p>
     */

    final protected static String qs =
	"\\\"(?:\\\\\"|[^\\p{Cntrl}\\\"])*\\\""
	;
    
    /**
     * Pattern used to match the type/subtype of a MIME expression.
     * The matched groups are numbered by the opening parentheses in
     * the pattern.  The different matching groups are:<ul>
     *
     * <li> group(0) : the matched input.
     * 
     * <li> group(1) : type
     * 
     * <li> group(2) : subtype
     * 
     * <li> group(3) : the rest of the input, to which you then apply
     * {@link #m_p2}.
     * 
     * </ul>
     */
    static protected Pattern m_p1 = null;

    /**
     * Pattern used to match the optional parameters of a MIME
     * expression.  The matched groups are numbered by the opening
     * parentheses in the pattern. The source for the pattern is the
     * parameters as identified by {@link #m_p1}.  The different
     * matching groups are:<ul>
     *
     * <li> group( 0 ) : parameter ( attribute=value ).
     * 
     * <li> group( 1 ) : attribute (parameter name).
     * 
     * <li> group( 2 ) : value for that parameter.
     * 
     * </ul>
     *
     * Note: You should match this pattern repeatedly until the input
     * is exhausted.<p>
     */

    static protected Pattern m_p2 = null;

    /**
     * Initialization for shared {@link Pattern} object that matches
     * valid MIME type expressions.
     */

    static protected void init()
    {

	try {
	    
	    if( m_p1 != null && m_p2 != null ) return;
	    
	    m_p1 = Pattern.compile
		( "^("+tok+")/("+tok+")(.*)$"
		  );
	    
	    m_p2 = Pattern.compile
		( "\\s*;\\s*("+tok+")=("+tok+"|"+qs+")\\s*"
		  );

	}

	catch( PatternSyntaxException ex ) {

	    /* Masquerade this exception so that it does not show up
	     * on method signatures throughout this and other
	     * packages.  The decision to use java.util.regex here is
	     * an implementation decision and its exception signatures
	     * should not be propagated.
	     */

	    AssertionError err = new AssertionError
		( "Could not compile regex patterns."
		  );

	    err.initCause( ex );

	    throw err;

	}


    }

    /**
     * Returns the MIME type parameter value as either an HTTP
     * <code>token</code> or HTTP <code>quoted-string</code> depending
     * on whether or not it contains any characters that need to be
     * escaped.
     *
     * @param force When true the returned value is always a
     * <code>quoted-string</code>.
     */
    
    static public String quoteString
	( String value,
	  boolean force
	  )
    {
	
	StringBuffer sb = new StringBuffer();
	
	int len = value.length();
	
	boolean didEscape = false;
	
	for( int i=0; i<len; i++ ) {
	    
	    char ch = value.charAt( i );
	    
	    if( isHttpCtlChar( ch ) ||
		isHttpSeparatorChar( ch )
		) {
		
		sb.append( '\\' );
		
		didEscape = true;
		
	    }
	    
	    sb.append( ch );
	    
	}
	
	return ( didEscape || force )
	    ? "\""+sb.toString()+"\""
	    : sb.toString()
	    ;
	
    }
    
    /**
     * If the value is an HTTP <code>quoted-string</code> then we
     * strip of the quote characters now and translate any escaped
     * characters into themselves, e.g., '\"' => '"'.  Otherwise
     * returns <i>value</i>.
     *
     * @param IllegalArgumentException If the value is a malformed
     * quoted string.  For example, no closing quote character or no
     * character after an escape character.
     */
	
    static public String unquoteString( String value )
	throws IllegalArgumentException
    {
	
	String originalValue = value; // save.
	
	/* Do nothing unless a quoted-string.
	 */
	
	if( ! value.startsWith( "\"" ) ) {
	    
	    return value;
	    
	}
	
	/* Drop off the quote characters.
	 */
	
	if( ! value.endsWith( "\"" ) ) {
	    
	    throw new IllegalArgumentException
		( "Quoted string does not end with '\"'"+
		  " : "+originalValue
		  );
	    
	}
	
	// Chop off the quote characters.
	
	value = value.substring
	    ( 1,
	      value.length() - 1
	      );
	
	// Translate escaped characters.
	
	StringBuffer sb = new StringBuffer();
	
	int len = value.length();
	
	for( int i=0; i<len; i++ ) {
	    
	    char ch = value.charAt( i );
	    
	    if( ch == '\\' ) {
		
		i++;
		
		if( i < len ) {
		    
		    ch = value.charAt( i );
		    
		    sb.append( ch );
		    
		} else {
		    
		    throw new IllegalArgumentException
			( "Escape character at end of string"+
			  " : "+originalValue
			  );
		    
		}
		
	    } else {
		
		sb.append( ch );
		
	    }
	    
	}
	
	return sb.toString();
	
    }

    /**
     * HTTP permits headers whose grammar is a comma delimited list to
     * be specified multiple times in an HTTP request.  This method
     * propertly combines the specified values need into a {@link
     * String} containing a comma-delimited list that preserves the
     * order in which the header values were specified.
     *
     * @param values E.g., as returned by {@link
     * javax.servlet.http#getHeaders( String name )}.
     *
     * @param defaultValue IFF <i>enum</i> is an empty enumeration,
     * then this value is returned to the caller.
     */

    public static String combineHeaders
	( Enumeration values,
	  String defaultValue
	  )
    {

	if( ! values.hasMoreElements() ) {

	    return defaultValue;

	}

	StringBuffer sb = new StringBuffer();

	boolean first = true;

	while( values.hasMoreElements() ) {

	    String value = (String)values.nextElement();
	    
	    if( ! first ) {

		sb.append( ", " );

	    }

	    sb.append( value );

	    first = false;

	}

	return sb.toString();

    }

    /**
     * Splits out the elements for an HTTP header value whose grammar
     * is a comma delimited list.
     */

    public static String[] splitCommaList
	( String value
	  )
    {

	log.debug
	    ( "Header-Value: "+value
	      );

	String[] values = value.split
	    ( "\\s*,\\s*"
	      );

	log.debug
	    ( "Found "+values.length+" elements in list."
	      );

	return values;

    }

}
