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

import java.util.Vector;
import java.util.regex.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Arrays;

import org.CognitiveWeb.util.CaseInsensitiveStringComparator;

import org.apache.log4j.Logger;

/**
 * Parser for MIME type data.  MIME type, subtype, and MIME parameter
 * names are case-insensitive.  Parameters values are treated as
 * case-sensitive by this class, but subclasses may override the
 * comparison logic for parameters, e.g., the "q" parameter in {@link
 * AcceptMIMEType} has a default value and its values are compared
 * after conversion into a floating point number.<p>
 *
 * @todo Consider requiring all default parameters to be made explicit
 * during the constructor / parse of the MIMEType.  It might greatly
 * simplify the logic if we treated the "default" as a syntactic sugar
 * that did NOT survive a round trip.  Or survived by virtue of being
 * marked with a 'defaultValue' flag on a per-parameter-type (or
 * per-parameter-instance) basis by a subclass.<p>
 *
 * @todo IF MIME is constrained to not have duplicate parameter names
 * then (a) we should check for that and throw an error when
 * duplicates exist; and (b) we can change the {@link #getParams()}
 * return value to an (immutable) {@link Map}.  This sort of logic
 * already partly exists in {@link #getIntersection( MIMEType other,
 * String[] dropParams )}.
 *
 * @todo Consider migrating the NVPair[] to a Vector of NVPair in
 * order to provide for a mutable implementation of {@link MIMEType},
 * in which case we will also need a wrapper that provides an
 * immutable view. However, I don't necessarily want to expose a
 * mutable iterface as fat as {@link Vector}, so this may not be a
 * good idea.  See {@link AcceptMIMEType}.
 *
 * @todo Add support for mutable parameters, including addParam(
 * String name, String value ), removeParam( String name ), and
 * setParam( String name, String value ).  If duplicate parameter
 * names are not permitted by MIME, then perhaps we can simplify this
 * interface.  Derive an ImmutableMIMEType class for people who don't
 * want their MIMEType expressions to be changable.
 */

public class MIMEType
{

    /**
     * The {@link Logger} for {@link MIMEType} operations.  The {@link
     * Logger} is named for this class.
     */

    protected static final Logger log = Logger.getLogger
	( MIMEType.class
	  );

    String   m_type;
    String   m_subtype;
    NVPair[] m_params = EMPTY;

    // Split out base value and parameters.  These
    // guidelines were extracted from 3.6 and 3.7 of the
    // HTTP/1.1 specification.
    //	
    // No LWS between type and subtype, both are required.
    // Each parameter must specify an attribute and a
    // value.  No LWS between the attribute and the value.
    // type/subtype [ ';' parameter ]*
    //
    // type      := token
    // subtype   := token
    // parameter := attribute '=' value
    // attribute := token
    // value     := token | quoted-string
    //
    
    /**
     * An empty NVPair[] used by the parser when there are no MIME
     * parameters.
     */

    final static NVPair[] EMPTY = new NVPair[]{};

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

	    String tok = HTTPHeaderUtility.httpTokenPattern;
	    
	    String qs = HTTPHeaderUtility.httpQuotedStringPattern;

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
     * Returns the names of all default parameters recognized by this
     * class for the MIME type and subtype represented by this {@link
     * MIMEType} instances.
     *
     * @return The default implementation always returns an empty
     * {@link String}[] since it does not recognize any default MIME
     * type parameters.  Implementors that subclass {@link MIMEType}
     * MUST extend this method if the subclass recognizes any default
     * MIME type parameters.
     */

    public String[] getDefaultParamNames()
    {

	return new String[]{};

    }

    /**
     * Returns true iff the named parameter should be ignored by the
     * <code>spans</code> logic for MIME type and subtype represented
     * by this {@link MIMEType} instance.
     *
     * @return The default implementation always returns
     * <code>false</code>.  Implementors that subclass {@link
     * MIMEType} MUST extend this method if the subclass contains any
     * parameters that should be ignored by the <code>spans</code>
     * logic.
     */

    public boolean isIgnoredParam
	( String name
	  )
    {

	return false;

    }

    /**
     * Clones the specified {@link MIMEType}.
     */

    public MIMEType( MIMEType mt )
    {

	this( mt.toString() );

    }

    /**
     * Constructs a {@link MIMEType} object from its component parts.
     * The {@link MIMEType} will not have any MIME attributes.
     */

    public MIMEType( String type, String subtype )
    {
	this( type, subtype, EMPTY );
    }

    /**
     * Constructs a {@link MIMEType} object from its component
     * parts.<p>
     *
     * @param type The media type or "*" (if the <i>subtype</i> if
     * also "*").
     *
     * @param subtype The media subtype or "*".
     *
     * @param params An array of name-value pairs specifying the MIME
     * attributes.
     *
     * @exception IllegalArgumentException If the method is unable to
     * validate the syntax of the type, subtype and params.
     * Everything must be an HTTP <code>token</code> except the
     * attribute value, which is interchanged as an HTTP
     * <code>quoted-string</code> any may contain pretty much any
     * character sequence.  Also thrown if <i>params</i> is
     * <code>null</code>.
     */

    public MIMEType
	( String type,
	  String subtype,
	  NVPair[] params
	  )
	throws IllegalArgumentException
    {
	
 	init();
	
	m_type    = type;
	m_subtype = subtype;
	m_params  = params;
	
	if( ! HTTPHeaderUtility.isHttpToken( type ) ) {
	    
	    throw new IllegalArgumentException
		( "MIME type is not an HTTP token"+
		  " : '"+type+"'"
		  );
	    
	}
	
	if( ! HTTPHeaderUtility.isHttpToken( subtype ) ) {
	    
	    throw new IllegalArgumentException
		( "MIME subtype is not an HTTP token"+
		  " : '"+subtype+"'"
		  );
	    
	}
	
	if( params == null ) {
	    
	    throw new IllegalArgumentException
		( "params may not be null."
		  );

	}
	
	for( int i=0; i<params.length; i++ ) {
	    
	    String attribute = params[i].getName();
		
	    String value = params[i].getValue();
	    
	    if( ! HTTPHeaderUtility.isHttpToken( attribute ) ) {
		
		throw new IllegalArgumentException
		    ( "MIME attribute is not an HTTP token"+
		      " : '"+attribute+"'"
		      );
		
	    }
	    
	}
	
    }

    /**
     * Constructor parses the string as a MIME Internet Type
     * expression.  The results of the parse are available from the
     * various methods on the constructed object.<p>
     *
     * From the HTTP/1.1 specification: The type, subtype, and
     * parameter attribute names are case-insensitive. Parameter
     * values might or might not be casesensitive, depending on the
     * semantics of the parameter name. Linear white space (LWS) MUST
     * NOT be used between the type and subtype, nor between an
     * attribute and its value.<p>
     *
     * @exception IllegalArgumentException Indicates that the
     * string could not be parsed as a valid MIME expression.
     */

    public MIMEType( String s )
	throws IllegalArgumentException
    {
	
 	init();

	log.debug
	    ( "Parsing: '"+s+"'"
	      );
	
	Matcher m1 = m_p1.matcher( s );
	
	if( ! m1.matches() ) {

	    throw new IllegalArgumentException
		( "Can not parse '"+s+"' as a MIME string."
		  );

	}
	
	m_type = m1.group( 1 );

	log.debug
	    ( "type = '"+m_type+"'"
	      );
	
	m_subtype = m1.group( 2 );

	log.debug
	    ( "subtype = '"+m_subtype+"'"
	      );

	if( m_type.equals("*") && ! m_subtype.equals("*") ) {

	    throw new IllegalArgumentException
		( "The mime type wildcard is '*/*'"+
		  ", not "+m_type+"/"+m_subtype
		  );

	}

	String parameters = m1.group( 3 );

	log.debug
	    ( "parameters = '"+parameters+"'"
	      );
	
	/* If there are no parameters then we are done.
	 *
	 * Note: leftover whitespace at this point is illegal.
	 */
	
	if( parameters.length() == 0 ) {
	    
	    m_params = EMPTY;
	    
	    return;
	    
	}

	/* Repeatedly apply a pattern that matches on parameter each
	 * time.
	 */

	Vector v = new Vector();

	Matcher m2 = m_p2.matcher( parameters );

	int nextStart = 0;

	log.debug
	    ( "input length = "+parameters.length()
	      );

	while( m2.find() ) {
	    
	    log.debug
		( "nextStart = "+nextStart+"\n"+
		  "    start = "+m2.start()+"\n"+
		  "      end = "+m2.end()+"\n"+
		  "    match = '"+m2.group(0)+"'\n"+
		  "attribute = '"+m2.group(1)+"'\n"+
		  "    value = '"+m2.group(2)+"'\n"
		  );
	    
	    if( m2.start() != nextStart ) {

		throw new AssertionError
		    ( "Pattern matches are not contiguous"+
		      ", data='"+s+"'"+
		      ", position="+nextStart
		      );

	    }

	    nextStart = m2.end();

	    String attribute = m2.group( 1 );
	    
	    String value     = m2.group( 2 );
	    
	    value = HTTPHeaderUtility.unquoteString( value );

	    log.debug
		( "parameter : "+attribute+"="+value
		  );
	    
	    v.add( new NVPair( attribute, value ) );
	    
	}
	
	/* Make sure that nothing is left over.
	 */

	if( nextStart != parameters.length() ) {

	    throw new IllegalArgumentException
		( "Pattern did not completely absorb input"+
		  ", data='"+s+"'"+
		  ", nextStart="+nextStart+
		  ", inputLength="+parameters.length()
		  );

	}

	m_params = (NVPair[])v.toArray( EMPTY );
		
    }
    
    /**
     * Returns the MIME type, eg, "text" for "text/plain".
     */

    public String getType() {return m_type;}
    
    /**
     * Returns the MIME subtype, eg, "plain" for "text/plain".
     */

    public String getSubtype() {return m_subtype;}

    /**
     * Returns true iff the MIME type is "*", indicating a MIME type
     * wildcard.
     */

    public boolean isTypeWildcard() {
	return getType().equals("*");
    }

    /**
     * Returns true iff the MIME subtype is "*", indicating a MIME
     * wildcard.
     *
     * @deprecated Use {@link #isSubtypeWildcard()} instead.
     */

    public boolean isWildcard() {
	return isSubtypeWildcard();
    }

    /**
     * Returns true iff the MIME subtype is "*", indicating a MIME
     * subtype wildcard.
     */

    public boolean isSubtypeWildcard() {
	return getSubtype().equals("*");
    }

    /**
     * Returns the MIME type and subtype as "type/subtype", but
     * does not format in any MIME parameters.<p>
     *
     * Note: There is no LWS (linear whitespace) between the type
     * and the subtype for a MIME string.
     *
     * @see #toString()
     */
    
    public String getMimeType() {
	return getType()+"/"+getSubtype();	
    }

    /**
     * Convenience method for {@link #matches( MIMEType mimeType,
     * boolean matchParams )} that does NOT compare the MIME type
     * parameters.
     */

    public boolean matches( String mimeType )
	throws IllegalArgumentException
    {

// 	/* If the two string representations are the same then this is
// 	 * a match.  Otherwise we have to look further.
// 	 */

// 	if( getMimeType().equalsIgnoreCase( mimeType ) ) {
	    
// 	    return true;
	    
// 	}
	
	/* Otherwise we have to parse the MIME type expression and
	 * compare the components directly.
	 */

	return matches( new MIMEType( mimeType ),
			false
			);

    }

    /**
     * Convenience method for {@link #matches( MIMEType mimeType,
     * boolean matchParams )} that does NOT compare the MIME type
     * parameters.
     */

    public boolean matches( MIMEType otherMimeType )
	throws IllegalArgumentException
    {

	return matches( otherMimeType,
			false
			);

    }

    /**
     * Convenience method compares type, subtype, and type parameters
     */

    public boolean isExactMatch
	( MIMEType otherType
	  )
    {

	return isExactMatch
	    ( otherType,
	      true
	      );

    }

    /**
     * Convenience method compares type, subtype, and type parameters
     */

    public boolean isExactMatch
	( String otherType
	  )
	throws IllegalArgumentException
    {

	return isExactMatch
	    ( new MIMEType( otherType ),
	      true
	      );

    }

    /**
     * Returns true IFF the two MIME type expressions have the same
     * meaning.  I.e., they impose the same type and subtype
     * constraint and, optionally, they impose the same MIME type
     * parameter constraints (if any).  The MIME type and subtype and
     * MIME parameter names are compared using
     * <strong>case-insensitive</strong> comparison.<p>
     *
     * Note: The MIME parameter values are compared using a
     * case-sensitive comparison, which is not correct for all MIME
     * types since some explictly provide for case-insensitive
     * semantics for their parameter values.  Implementors are free to
     * further specialize this method in derived subclasses to obtain
     * MIME type specific comparision of MIME type parameter values or
     * to establish default parameter values.<p>
     *
     * Note: Defaults may be provided for MIME parameters through
     * subclasses by extending {@link #sameParamValue ( String name,
     * String realValue, String otherValue )}, {@link
     * #getDefaultParamNames()}, and {@link #isIgnoredParamName(
     * String name )}.<p>
     */

    public boolean isExactMatch
	( MIMEType t,
	  boolean compareParams
	  )
    {

	/* Compare the MIME type, MIME subtype since this is quick.
	 */

	if( ! getType().equalsIgnoreCase( t.getType() ) ) {

	    return false;

	}

	if( ! getSubtype().equalsIgnoreCase( t.getSubtype() ) ) {

	    return false;

	}

	if( compareParams ) {

	    /* Comparing parameters is not easy since there can be
	     * default parameters that are only recognized by specific
	     * subclasses.  Therefore we use the spans semantics to
	     * conclude that A == B IFF A-spans-B and B-spans-A.
	     */
	    
	    if( this.spans( t, true ) && t.spans( this, true ) ) {

		/* @todo Should we explictly test the isIgnoredParams
		 * here?
		 */

		return true;
		
	    } else {

		return false;

	    }

	} else {

	    return true;

	}

// 	if( compareParams ) {

// 	    /* Note: Do NOT test the count of parameters since a
// 	     * subclass may recognized some default MIME type
// 	     * parameters.
// 	     */

// // 	/* Make sure that each parameter exists and has the same
// // 	 * value.
// // 	 */

// // 	if( getParamCount() != t.getParamCount() ) {

// // 	    return false;

// // 	}
	    
// 	    NVPair[] params = getParams();
	    
// 	    for( int i=0; i<params.length; i++ ) {
		
// 		String attribute = params[i].getName();
		
// 		String realValue = params[i].getValue();
		
// 		String otherValue = t.getParamValue( attribute );

// 		if( realValue == null ) {
		    
// 		    return false;
		    
// 		}
		
// 		if( ! sameParamValue( attribute, realValue, otherValue ) ) {
		    
// 		    return false;
		    
// 		}
		
// 	    }

// 	}
	    
// 	return true;

    }

    /**
     * Returns true IFF this {@link MIMEType} has the specified
     * parameter value.  This method is exposed to account for any
     * MIME type specific case-sensitivity for the parameter name or
     * parameter value and any default MIME type parameters.<p>
     *
     * The default implementation uses a case-sensitive comparison of
     * the MIME parameter value.  Implementors are free to created
     * subclasses that extend this method to incorporate MIME type
     * specific logic in their comparison.<p>
     *
     * @param name The name of the parameter whose value is being
     * compared.  This parameter is ignored by this implementation but
     * is provided for subclasses that may have type specific value
     * comparison logic and/or parameter value default logic.
     *
     * @param realValue The value of the parameter on this {@link
     * MIMEType} object as discovered, e.g., by {@link #getParamValue(
     * String name )}.  This may explicitly be <code>null</code>.
     * Subclasses that have a default value for this named parameter,
     * e.g., {@link AcceptMIMEType} which has a default value for the
     * "q" parameter, MUST substitute their default value when
     * <i>realValue</i> is <code>null</code>.
     *
     * @param otberValue The value that is being compared to the
     * <i>foundValue</i>.  This parameter MAY be <code>null</code>, in
     * which case a subclass MAY choose to declare a default value to
     * be used in this comparison.
     *
     * @todo Consider defining this as spansParamValue so that it can
     * be used to test constraints that define a range of values
     * rather than just a point value.
     */

    public boolean sameParamValue
	( String name,
	  String realValue,
	  String otherValue
	  )
	throws IllegalArgumentException
    {

	if( realValue == null && otherValue != null ) {

	    return false;

	}

	return realValue.equals	// case-sensitive comparison.
	    ( otherValue
	      );

    }

    /**
     * This method may be extended to produce a specific
     * represensation of the value of the indicated parameter.  The
     * representation returned will still be quoted iff necessary for
     * HTTP by {@link #toString()} when serializing the entire header
     * value.
     */

    public String toString
	( String name,
	  String value
	  )
    {

	return value;

    }

    //************************************************************
    //************************************************************
    //************************************************************


    /**
     * Returns the intersection of the this media-range and the given
     * media-range.  The resulting {@link MIMEType} MAY be a fully
     * determined media type, but it MAY be undetermined, e.g., has a
     * type or subtype wildcard, does not specify a content encoding,
     * language family, etc.  If this is NOT a fully determined media
     * type, then the server MUST make it into a concrete media type.
     *
     * @param other The other media-range whose intersection with this
     * media-range will be return.
     *
     * @param dropParams An array of zero or more parameter names that
     * will be dropped out of the intersected media-ranges.  For
     * example, this is used to drop the "q" parameter during content
     * negotiation when computing the negotiated joint media-range.
     *
     * @return The intersection of the two media-ranges or
     * <code>null</code> if the intersection does not exist.  For
     * example, if the two media-ranges each specify different
     * concrete subtypes, such as "text/xml" vs "text/plain".
     */
    
    public MIMEType getIntersection
	( MIMEType other,
	  String[] dropParams
	  )
    {

	if( other == null ) {

	    throw new IllegalArgumentException
		( "The 'other' parameter may not be null."
		  );

	}

	if( dropParams == null ) {

	    throw new IllegalArgumentException
		( "The 'dropParams' parameter may not be null."
		  );

	}

	/* If the types agree, then choose either one.  If either's
	 * type is a wildcard, then choose the other's type.
	 * Otherwise there is no intersection.
	 */
	
	String type;

	if( getType().equalsIgnoreCase( other.getType() ) ) {
	    
	    type = getType();

	} else if( isTypeWildcard() ) {

	    type = other.getType();
	    
	} else if( other.isTypeWildcard() ) {

	    type = getType();

	} else {

	    return null;	// no intersection !

	}

	/* If the subtypes agree, then choose either one.  If either's
	 * subtype is a wildcard, then choose the other's subtype.
	 * Otherwise there is no intersection.
	 */

	String subtype;

	if( getSubtype().equalsIgnoreCase( other.getSubtype() ) ) {
	    
	    subtype = getSubtype();

	} else if( isSubtypeWildcard() ) {

	    subtype = other.getSubtype();
	    
	} else if( other.isSubtypeWildcard() ) {

	    subtype = getSubtype();

	} else {

	    return null;	// no intersection !

	}
	
	/* Place all parameters into a shared Map, except any
	 * parameters that are being dropped.  If the same parameter
	 * name occurs in both media-ranges, then the parameters MUST
	 * have the same value otherwise the intersection is
	 * null.
	 */

	Map params = new TreeMap // case-insensitive !!!
	    ( new CaseInsensitiveStringComparator()
	      );

	// Copy parameters from this media-range.

	if( true ) {	// local variables.

	    Iterator itr = Arrays.asList
		( getParams()
		  ).iterator()
		;
	    
	    while( itr.hasNext() ) {
		
		NVPair param = (NVPair)itr.next();

		String name = param.getName();

		boolean dropThisParam = false;

		for( int i=0; i<dropParams.length; i++ ) {

		    if( name.equalsIgnoreCase( dropParams[ i ] ) ) {

			dropThisParam = true;

			break;

		    }

		}
		
		if( ! dropThisParam ) {

		    params.put
			( name,
			  param
			  );

		}
		
	    }
	    
	}

	/* Copy parameters from the 'other' media-range.  The
	 * intersection fails if any shared parameter does not have
	 * the same value.
	 */

	if( true ) {	// local variables.
	    
	    Iterator itr = Arrays.asList
		( other.getParams()
		  ).iterator()
		;
	    
	    while( itr.hasNext() ) {
		
		NVPair param = (NVPair)itr.next();

		String name = param.getName();

		boolean dropThisParam = false;

		for( int i=0; i<dropParams.length; i++ ) {

		    if( name.equalsIgnoreCase( dropParams[ i ] ) ) {

			dropThisParam = true;

			break;

		    }

		}
		
		if( ! dropThisParam ) {

		    NVPair existingParam = (NVPair)params.get
			( name
			  );

		    /* Make sure that this value does not create a
		     * conflict with any existing value for the same
		     * parameter.
		     */

		    if( existingParam != null &&
			! sameParamValue
			( name,
			  existingParam.getValue(),
			  param.getValue()
			  ) ) {
			
			return null; // null intersection.

		    }

		    params.put
			( name,
			  param
			  );

		}
		
	    }
	    
	}

	/* Build the new MIMEType from the intersection of the
	 * parameters.
	 */

	StringBuffer sb = new StringBuffer();
	
	if( true ) {	// local variables.
	    
	    sb.append( type + "/" + subtype );
	    
	    Iterator itr = params.entrySet().iterator();
	    
	    while( itr.hasNext() ) {
		
		Map.Entry entry = (Map.Entry)itr.next();
		
		String name = (String)entry.getKey();
		
		NVPair param = (NVPair)entry.getValue();
		
		sb.append
		    ( ";"+param.getName()+"="+param.getValue()
		      );
		
	    }
	    
	}
	
	return new MIMEType
	    ( sb.toString()
	      );
	
    }


    //************************************************************
    //********************* SPAN LOGIC ***************************
    //************************************************************

    /**
     * Convenience method compares type, subtype, and type parameters
     */

    public boolean spans
	( String other
	  )
    {

	return spans( other, true );
	
    }

    /**
     * Convenience method compares type, subtype, and type parameters
     */

    public boolean spans
	( MIMEType other
	  )
    {

	return spans( other, true );
	
    }

    /**
     * Convenience method compares type, subtype, and optionally any
     * type parameters.
     */

    public boolean spans
	( String other,
	  boolean compareParams
	  )
    {
	
	return spans( new MIMEType( other ),
		      compareParams
		      );

    }

    /**
     * Returns true IFF this {@link MIMEType} expresses a constraint
     * that spans the <i>other</i> {@link MIMEType}.  The semantics of
     * "spans" are that the <i>other</i> may be either the same {@link
     * MIMEType} or may be a more constrained (aka specific) {@link
     * MIMEType}.
     *
     * @param compareParams IFF true the MIME type parameters are also
     * compared by this method.  The specific parameter values are
     * compared using {@link #sameParamValue( String name, String
     * realValue, String otherValue )}.
     */

    public boolean spans
	( MIMEType other,
	  boolean compareParams
	  )
    {

	/* A type wildcard (* / *) spans everything.
	 */

	if( isTypeWildcard() ) {

	    return true;

	}

	if( getType().equalsIgnoreCase( other.getType() ) &&
	    ( getSubtype().equalsIgnoreCase( other.getSubtype() ) ||
	      isSubtypeWildcard()
	      )
	    ) {

	    if( compareParams ) {

		/* 1. This MIMEType may NOT specify any constraints
		 * that are not also found on the "other" MIMEType.
		 * E.g., "text/xml;a=b" does NOT span "text/xml".
		 *
		 * 2. The "other" MIMEType may specify additional
		 * constraints (as type parameters), but it may not
		 * specify any type parameter that is given a
		 * different value by this MIMEType.
		 *
		 * Note: We can NOT compare the #of parameters since
		 * subclasses may recognize default parameters.
		 */

// 		if( getParamCount() > other.getParamCount() ) {

// 		    return false;

// 		}

		NVPair[] params = getParams();

		for( int i=0; i<params.length; i++ ) {

		    String name = params[ i ].getName();

		    if( isIgnoredParam( name ) ) {

			continue;

		    }

		    String value = params[ i ].getValue();
		    
		    String otherValue = other.getParamValue
			( name
			  );

// 		    if( value == null && otherValue == null ) {

// 			continue; // only a flag without a value.

// 		    }

		    if( ! sameParamValue( name, value, otherValue ) ) {

			return false;

		    }

		}

		/* Now check each recognized default parameter name.
		 * If the default parameter is explictly bound then
		 * this was already checked about.  However, it the
		 * parameter was not explicitly bound, then it needs
		 * to be tested here.
		 */

		String[] defaultNames = getDefaultParamNames();

		for( int i=0; i<defaultNames.length; i++ ) {

		    String name = defaultNames[ i ];

		    if( isIgnoredParam( name ) ) {

			continue;

		    }

		    String value = getParamValue
			( name
			  );
		    
		    String otherValue = other.getParamValue
			( name
			  );

		    if( ! sameParamValue( name, value, otherValue ) ) {

			return false;

		    }

		}

	    }

	    return true;

	}

	return false;

    }

    /**
     * Convenience method for <code>new MIMEType( other ).spans( this,
     * true );</code>
     */

    public boolean isSpannedBy
	( String other
	  )
    {

	return new MIMEType( other ).spans( this, true );

    }

    /**
     * Convenience method for <code>other.spans( this, true );</code>
     */

    public boolean isSpannedBy
	( MIMEType other
	  )
    {

	return other.spans( this, true );

    }

    /**
     * Convenience method for <code>other.spans( this, compareParams );</code>
     */

    public boolean isSpannedBy
	( MIMEType other,
	  boolean compareParams
	  )
    {

	return other.spans( this, compareParams );

    }

    /**
     * Matches if this {@link MIMEType} has the same type and subtype
     * or if the types match and <i>this</i> {@link MIMEType} is a
     * wildcard that covers the given <i>mimeType</i>.  The parameters
     * are optionally compared by this method.<p>
     *
     * @param compareParams IFF true the MIME type parameters are also
     * compared by this method.  The specific parameter values are
     * compared using {@link #sameParamValue( String name, String
     * realValue, String otherValue )}.
     *
     * Note: This is a convenience for <code>spans( other, false
     * );</code>.
     *
     * @see #spans( MIMEType other, boolean compareParams )
     */	

    public boolean matches
	( MIMEType other,
	  boolean compareParams
	  )
    {

	return spans( other, false );

// 	if( getType().equalsIgnoreCase( mimeType.getType() ) &&
// 	    ( getSubtype().equalsIgnoreCase( mimeType.getSubtype() ) ||
// 	      isSubtypeWildcard()
// 	      )
// 	    ) {

// 	    return true;

// 	}

// 	return false;

    }

    /**
     * Returns true iff the MIME type, MIME subtype and any MIME
     * parameters are all the same as determined by {@link
     * #isExactMatch( MIMEType other, boolean compareParams )} when
     * <i>compareParams</i> is <code>true</code>.
     *
     * @param o Must be a {@link String} or {@link MIMEType} object.
     */

    public boolean equals( Object o )
    {
	
	MIMEType t = null;

	
	if( o instanceof String ) {
	    
	    t = new MIMEType( (String) o );
		
	} else if( o instanceof MIMEType ) {
	    
	    t = (MIMEType) o;
	    
	} else {
	    
	    throw new IllegalArgumentException
		( "Can't compare "+o.getClass()
		  );
	    
	}
	
	return isExactMatch
	    ( t,
	      true
	      );
	
    }
	
//     /**
//      * Returns an immutable {@link Iterator} that visits the MIME
//      * parameters, each of which is a name-value pair represented
//      * using an {@link NVPair} instance.<p>
//      *
//      * Note: If a subclass declares any default parameters, then it
//      * MAY choose to return them here.<p>
//      *
//      * @return An immutable {@link Iterator}.  If there are no MIME
//      * parameters, then the {@link Iterator} will visit zero elements.
//      */

//     public Iterator getParams()
//     {
	
// 	return Arrays.asList( m_params ).iterator();
    
//     }

    /**
     * An array of the explicitly declared MIME type parameters.
     * Default parameters are NOT represented in the returned array.
     *
     * @todo This returns the backing array.  Should this interface be
     * changed to increase the encapsulation?  We MUST change this
     * implementation if we want to enforce an immutable interface.
     */

    public NVPair[] getParams()
    {

	return m_params;

    }

    /**
     * Returns the nth MIME type parameters, origin zero.
     */
    
    public NVPair getParam( int i ) {return m_params[ i ];}
    
    /**
     * Returns the #of MIME parameters.
     */
    
    public int getParamCount() {return m_params.length;}
    
    /**
     * Returns the value for the indicated name and <code>null</code>
     * if there is no MIME parameter with that name.<p>
     *
     * @param name The name of a MIME parameter, which is
     * case-insensitive.  Implementors can create subclasses if a
     * specific MIME type declares some case-sensitive MIME type
     * parameter names.<p>
     */
    
    public String getParamValue
	( String name
	  )
    {
	
	if( name == null ) {
	    
	    throw new IllegalArgumentException
		( "name may not be null."
		  );
	    
	}
	
	NVPair[] params = getParams();
	
	for( int i=0; i<params.length; i++ ) {
	    
	    String attribute = params[i].getName();
	    
	    if( name.equalsIgnoreCase( attribute ) ) {
		
		return params[i].getValue();
		
		}
	    
	}
	
	return null;
	
    }

    /**
     * Generates a MIME type string from the parsed data.  If any
     * parameter value is not a valid HTTP <code>token</code> then it
     * is serialized as an HTTP <code>quoted-string</code>.<p>
     *
     * Note: There is no LWS (linear whitespace) between the type and
     * the subtype for a MIME string.  Also, there is no LWS between
     * the attribute name and the attribute value, e.g., "charset=xxx"
     * but not "charset = xxx".
     */

    public String toString()
    {
	
	StringBuffer sb = new StringBuffer();
	
	sb.append( getMimeType() );
	
	NVPair[] params = getParams();
	
	for( int i=0; i<params.length; i++ ) {
	    
	    String attribute = params[i].getName();
	    
	    // Note: value represented as quoted-string iff
	    // necessary.
	    
	    String value = HTTPHeaderUtility.quoteString
		( params[i].getValue(),
		  false
		  );
	    
	    // Note: no LWS between attribute and value.
	    
	    sb.append( "; "+attribute+"="+value );
	    
	}
	
	return sb.toString();
	
    }

}
