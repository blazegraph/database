/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/

package com.bigdata.util.httpd;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.util.httpd.MIMEType;
import com.bigdata.util.httpd.NVPair;

/**
 * Test suite for {@link MIMEType}.
 */

public class TestMIMEType
    extends TestCase
{

    public static Test suite()
    {

        TestSuite suite = new TestSuite();

        suite.addTestSuite
	    ( TestMIMEType.class
	      );

        return suite;

    }

    final private NVPair[] EMPTY = new NVPair[]{};

    /**
     * Note: This class is reused by {@link TestAcceptMIMEType}.
     */

    abstract public static class Criteria
    {
	
	public String toString()
	{
	    
	    StringBuffer sb = new StringBuffer();

	    sb.append( "Criteria" );

	    sb.append( ": type="+getType() );
	    sb.append( ", subtype="+getSubtype() );
	    sb.append( ", params=" );

	    NVPair[] params = getParams();

	    for( int i=0; i<params.length; i++ ) {

		NVPair param = params[ i ];

		sb.append( "\""+param.getName()+"\"=\""+param.getValue()+"\""
			   );

	    }

	    return sb.toString();
		
	}

	abstract public String getType();
	abstract public String getSubtype();
	abstract public NVPair[] getParams();
	
	public void test( MIMEType t )
	    throws AssertionFailedError
	{

	    if( ! getType().equalsIgnoreCase( t.getType() ) ) {

		throw new AssertionFailedError
		    ( "MIME type should be "+getType()+
		      ", not "+t.getType()
		      );
		
	    }

	    if( ! getSubtype().equalsIgnoreCase( t.getSubtype() ) ) {

		throw new AssertionFailedError
		    ( "MIME subtype should be "+getSubtype()+
		      ", not "+t.getSubtype()
		      );
		
	    }

	    // should always be true.
	    assertEquals
		( t.getParams().length,
		  t.getParamCount()
		  );

	    if( getParams().length != t.getParamCount() ) {

		throw new AssertionFailedError
		    ( "Should be "+getParams().length+" MIME parameters"+
		      ", but found "+t.getParamCount()
		      );

	    }
	    
	    NVPair[] params = getParams();

	    for( int i=0; i<params.length; i++ ) {

		String attribute = params[i].getName();

		String realValue = params[i].getValue();

		String foundValue = t.getParamValue( attribute );

		if( foundValue == null ) {

		    throw new AssertionFailedError
			( "Should have found MIME parameter named "+
			  attribute
			  );

		}

		if( ! foundValue.equals( realValue ) ) {

		    throw new AssertionFailedError
			( "Should have value = '"+realValue+
			  " for MIME parameter named '"+attribute+"'"+
			  ", but found value = '"+foundValue+"'"
			  );

		}

	    }

	}

    }

    /**
     * Test helper method attempts to parse a MIME expression and then
     * validates the parse against the provided {@link Criteria} and
     * then tests for round-trip fidelity as well.
     */

    static public void doAcceptanceTest
	( String data,
	  Criteria criteria
	  )
    {

	/* First we test that the string representation of the MIME
	 * type expression can be parsed and that the result of that
	 * parse satisifies the specified Criteria.
	 */

	MIMEType t = null;

	try {

	    t = new MIMEType
		( data
		  );

	    criteria.test( t );

	}

	catch( Exception ex ) {

	    ex.printStackTrace( System.err );

	    throw new AssertionFailedError2
		( "data = '"+data+"'",
		  ex
		  );

	}

	/* Now we serialize the MIMEType object as a String and test
	 * that the serialized form can be re-parsed and still meets
	 * the same Criteria.
	 */

	String roundTripData = t.toString();

	try {

	    MIMEType t2 = new MIMEType
		( roundTripData
		  );

	    criteria.test( t2 );

	}

	catch( Exception ex ) {

	    ex.printStackTrace( System.err );

	    throw new AssertionFailedError2
		( "data = '"+data+"'"+
		  ", roundTripData = '"+roundTripData+"'",
		  ex
		  );

	}

    }

    /**
     * Test helper submits data that should produce an exception of
     * the indicated class.
     */
    static public void doRejectionTest
	( String data,
	  Class exceptionClass
	  )
    {

	try {

	    MIMEType t = new MIMEType
		( data
		  );

	}

	catch( Throwable t ) {

	    if( exceptionClass.isInstance( t ) ) return; // Ok.

	    // Show whatever was thrown.
	    t.printStackTrace( System.err );

	    throw new AssertionFailedError2
		( "Should have thrown "+exceptionClass+" instead"+
		  ", not "+t.getClass()+
		  " for data='"+data+"'",
		  t
		  );

	}

	throw new AssertionFailedError
	    ( "Should have thrown "+exceptionClass+
	      " for data='"+data+"'"
	      );
	
    }

    /**
     * Helper class.
     */
    public static class AssertionFailedError2
	extends AssertionFailedError
    {

	public AssertionFailedError2
	    ( String msg,
	      Throwable t
	      )
	{

	    super( msg );

	    initCause( t );

	}

    }

    /**
     * testAccept# : basic test suite for correct parsing of type,
     * subtype and parameters.
     */

    public void testAccept1_text_xml()
    {
	doAcceptanceTest
	    ( "text/xml",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {return EMPTY;}
		}
		);
    }

    public void testAccept1_audio_basic()
    {
	doAcceptanceTest
	    ( "audio/basic",
		new Criteria() {
		    public String getType() {return "audio";}
		    public String getSubtype() {return "basic";}
		    public NVPair[] getParams() {return EMPTY;}
		}
		);
    }

    public void testAccept2()
    {
	doAcceptanceTest
	    ( "text/xml; a=b",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "a", "b" )
			};
		    }
		}
		);
    }

    public void testAccept3()
    {
	doAcceptanceTest
	    ( "text/xml; a=\"b\"",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "a", "b" )
			};
		    }
		}
		);
    }

    public void testAccept4()
    {
	doAcceptanceTest
	    ( "text/xml; A=\"b\"",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "a", "b" )
			};
		    }
		}
		);
    }

    public void testAccept5()
    {
	doAcceptanceTest
	    ( "text/xml ; A=\"b\"; charset=\"UTF-8\"",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "a", "b" ),
			    new NVPair( "charset", "UTF-8" )
			};
		    }
		}
		);
    }

    public void testAccept6()
    {
	doAcceptanceTest
	    ( "text/xml; charset=\"UTF\\\"-8\"",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "charset", "UTF\"-8" )
			};
		    }
		}
		);
    }

    /**
     * multipart/mixed mime type test.
     */

    public void testAcceptMultipart_1()
    {
	doAcceptanceTest
	    ( "multipart/mixed"+
	      "; boundary=\"----\\=_Part_67_3945427.1085762166434\"",
	      new Criteria() {
		  public String getType() {return "multipart";}
		  public String getSubtype() {return "mixed";}
		  public NVPair[] getParams() {
		      return new NVPair[] {
			  new NVPair( "boundary",
				      "----=_Part_67_3945427.1085762166434"
				      )
		      };
		  }
	      }
	      );
    }

    /**
     * multipart/signed mime type test.
     */

    public void testAcceptMultipart_2()
    {
	doAcceptanceTest
	    ( "multipart/signed"+
	      "; protocol=\"application/pgp-signature\""+
	      "; micalg=pgp-sha1"+
	      "; boundary=\"Apple-Mail-3-78839501\"",
	      new Criteria() {
		  public String getType() {return "multipart";}
		  public String getSubtype() {return "signed";}
		  public NVPair[] getParams() {
		      return new NVPair[] {
			  new NVPair( "protocol",
				      "application/pgp-signature"
				      ),
			  new NVPair( "micalg",
				      "pgp-sha1"
				      ),
			  new NVPair( "boundary",
				      "Apple-Mail-3-78839501"
				      )
		      };
		  }
		}
	      );
    }

    /**
     * Basic acceptance tests for type and subtype wildcards.
     */

    public void testAccept_wildcard_subtype()
    {
	doAcceptanceTest
	    ( "text/*",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "*";}
		    public NVPair[] getParams() {return EMPTY;}
		}
		);
    }

    public void testAccept_wildcard_type_and_subtype()
    {
	doAcceptanceTest
	    ( "*/*",
		new Criteria() {
		    public String getType() {return "*";}
		    public String getSubtype() {return "*";}
		    public NVPair[] getParams() {return EMPTY;}
		}
		);
    }

    public void testReject_wildcard_type_only()
    {
	doRejectionTest
	    ( "*/plain",
	      IllegalArgumentException.class
	      );
    }

    /**
     * test_LWS_# : test suite for correct acceptance and correct
     * rejection of (linear whitespace).  Linear white space (LWS)
     * MUST NOT be used between the type and subtype, nor between an
     * attribute and its value.
     */

    public void test_LWS_1()
    {

	doRejectionTest
	    ( "text /xml",
	      IllegalArgumentException.class
	      );
    }

    public void test_LWS_2()
    {
	doRejectionTest
	    ( "text/ xml",
	      IllegalArgumentException.class
	      );
    }

    public void test_LWS_3()
    {
	doRejectionTest
	    ( "text /xml",
	      IllegalArgumentException.class
	      );
    }

    public void test_LWS_4()
    {
	doRejectionTest
	    ( " text/xml",
	      IllegalArgumentException.class
	      );
    }

    public void test_LWS_5()
    {
	doRejectionTest
	    ( "text/xml ",
	      IllegalArgumentException.class
	      );
    }

    // LWS after the value is Ok.
    public void test_LWS_6()
    {
	doAcceptanceTest
	    ( "text/xml; a=b ",
		new Criteria() {
		    public String getType() {return "text";}
		    public String getSubtype() {return "xml";}
		    public NVPair[] getParams() {
			return new NVPair[] {
			    new NVPair( "a", "b" )
			};
		    }
		}
	      );
    }

    // LWS between the attribute and the value is illegal.
    public void test_LWS_7()
    {
	doRejectionTest
	    ( "text/xml; a =b",
	      IllegalArgumentException.class
	      );
    }

    // LWS between the attribute and the value is illegal.
    public void test_LWS_8()
    {
	doRejectionTest
	    ( "text/xml; a= b",
	      IllegalArgumentException.class
	      );
    }

    // LWS between the attribute and the value is illegal.
    public void test_LWS_10()
    {
	doRejectionTest
	    ( "text/xml; a =\"b\"",
	      IllegalArgumentException.class
	      );
    }

    // LWS between the attribute and the value is illegal.
    public void test_LWS_11()
    {
	doRejectionTest
	    ( "text/xml; a= \"b\"",
	      IllegalArgumentException.class
	      );
    }

    /**
     * Tests for correct rejection of trailing semicolon.
     */
    public void test_separator_1()
    {
	doRejectionTest
	    ( "text/xml; ",
	      IllegalArgumentException.class
	      );
    }

    //************************************************************
    //********************** matches *****************************
    //************************************************************

    /**
     * Test verifies that a specific MIME type matches the same MIME
     * type when the latter is expressed as a {@link String}.
     */

    public void test_matches_1()
    {

	assertTrue
	    ( new MIMEType( "text/xml" ).matches
	      ( "text/xml"
		)
	      );

    }

    /**
     * Test verifies that a specific MIME type matches the same MIME
     * type when the latter is expressed as a {@link MIMEType}.
     */

    public void test_matches_2()
    {

	assertTrue
	    ( new MIMEType( "text/xml" ).matches
	      ( new MIMEType( "text/xml" )
		)
	      );

    }

    /**
     * Test verifies that a wildcard subtype matches a specific MIME
     * type and subtype.
     */

    public void test_matches_3()
    {

	assertTrue
	    ( new MIMEType( "text/*" ).matches
	      ( new MIMEType( "text/plain;foo=bar" )
		)
	      );

    }

    /**
     * Test should succeed since the MIME parameters are being
     * ignored, but it would succeed even if the MIME parameters were
     * being compared.
     */

    public void test_matches_4()
    {

	assertTrue
	    ( new MIMEType( "text/xml" ).matches
	      ( new MIMEType( "text/xml;foo=bar" )
		)
	      );

    }

    /**
     * Verifies that the MIME parameters are not compared since the
     * test would fail if they were being compared.
     */

    public void test_matches_5()
    {

	assertTrue
	    ( new MIMEType( "text/xml;foo=bar" ).matches
	      ( new MIMEType( "text/xml" )
		)
	      );

    }

    //************************************************************
    //********************** equals ******************************
    //************************************************************

    /**
     * Test helper method for testing the {@link MIMEType#equals(
     * Object o )} implementation.<p>
     *
     * Note: This does NOT accept {@link String} MIME type expressions
     * so that it may be reused as part of the test harness for
     * classes that extend {@link MIMEType}.<p>
     */

    static public void doEqualTest
	( MIMEType t1,
	  MIMEType t2
	  )
    {

	try {

	    assertTrue( t1.equals( t2 ) );

	    assertTrue( t2.equals( t1 ) );

	}

	catch( Throwable ex ) {

	    throw new AssertionFailedError2
		( "Comparing equal"+
		  ": t1='"+t1+"'"+
		  ", t2='"+t2+"'",
		  ex
		  );

	}

    }

    /**
     * Test helper method for testing the {@link MIMEType#equals(
     * Object o )} implementation.<p>
     *
     * Note: This does NOT accept {@link String} MIME type expressions
     * so that it may be reused as part of the test harness for
     * classes that extend {@link MIMEType}.<p>
     */

    static public void doNotEqualTest
	( MIMEType t1,
	  MIMEType t2
	  )
    {

	try {

	    assertFalse( t1.equals( t2 ) );

	    assertFalse( t2.equals( t1 ) );

	}

	catch( Throwable ex ) {

	    throw new AssertionFailedError2
		( "Comparing NOT equal"+
		  ": t1='"+t1+"'"+
		  ", t2='"+t2+"'",
		  ex
		  );

	}

    }

    //
    // Testing correct acceptance of equal MIME type expressions.
    //

    public void test_equals_1()
    {
	doEqualTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/xml" )
	      );
    }

    public void test_equals_2()
    {
	doEqualTest
	    ( new MIMEType( "text/xml; a=b" ),
	      new MIMEType( "text/xml; a=b" )
	      );
    }

    public void test_equals_3()
    {
	doEqualTest
	    ( new MIMEType( "text/xml; a=b; c=d" ),
	      new MIMEType( "text/xml; c=d; a=b" )
	      );
    }

    public void test_equals_4()
    {
	doEqualTest
	    ( new MIMEType( "text/xml; a=b" ),
	      new MIMEType( "text/xml; a=\"b\"" )
	      );
    }

    //
    // Testing correct rejection of non-equal MIME type expressions.
    //

    public void test_not_equals_1()
    {
	doNotEqualTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/*" )
	      );
    }

    public void test_not_equals_2()
    {
	doNotEqualTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/xml; a=b" )
	      );
    }

    public void test_not_equals_3()
    {
	doNotEqualTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "application/xml" )
	      );
    }

    /**
     * This case shows that the default parameter "q" is NOT
     * recognized by the base {@link MIMEType} class.  However that
     * default parameter SHOULD be recognized by the {@link
     * AcceptMIMEType} - which is tested by {@link
     * TestAcceptMIMEType}.
     */

    public void test_not_equals_4()
    {
	doNotEqualTest
	    ( new MIMEType( "text/xml;q=1" ),
	      new MIMEType( "text/xml" )
	      );
    }



    //************************************************************
    //************************ spans *****************************
    //************************************************************

    /**
     * Test helper method for testing the {@link MIMEType#spans(
     * Object o, boolean compareParams )} implementation.<p>
     *
     * Note: This does NOT accept {@link String} MIME type expressions
     * so that it may be reused as part of the test harness for
     * classes that extend {@link MIMEType}.<p>
     */

    static public void doSpansTest
	( MIMEType t1,
	  MIMEType t2,
	  boolean compareParams
	  )
    {

	try {

	    assertTrue( t1.spans( t2,
				  compareParams
				  )
			);

	    // Note: While this is logically valid, in practice
	    // isExactMatch is implemented using spans(), and equals()
	    // is implemented using isExactMatch(), so the following
	    // code does not actually verify anything at all based on
	    // the grounding of the different method implementations.

// 	    if( t1.equals( t2 ) ) {
		
// 		/* IFF t1 == t2, then B also spans A.
// 		 */

// 		assertTrue( t2.spans( t1,
// 				      compareParams
// 				      )
// 			    );

// 	    }

	}

	catch( Throwable ex ) {

	    throw new AssertionFailedError2
		( "Comparing A-spans-B"+
		  ": t1='"+t1+"'"+
		  ", t2='"+t2+"'"+
		  ": compareParams="+compareParams,
		  ex
		  );

	}

    }

    /**
     * Test helper method for testing the {@link MIMEType#spans(
     * Object o, boolean compareParams )} implementation.<p>
     *
     * Note: This does NOT accept {@link String} MIME type expressions
     * so that it may be reused as part of the test harness for
     * classes that extend {@link MIMEType}.<p>
     */

    static public void doDoesNotSpanTest
	( MIMEType t1,
	  MIMEType t2,
	  boolean compareParams
	  )
    {

	try {

	    assertFalse( t1.spans( t2,
				   compareParams
				   )
			 );

	}

	catch( Throwable ex ) {

	    throw new AssertionFailedError2
		( "Comparing A-does-not-span-B"+
		  ": t1='"+t1+"'"+
		  ", t2='"+t2+"'"+
		  ": compareParams="+compareParams,
		  ex
		  );

	}

    }

    //
    // Testing correct acceptance and correct rejection of spanned
    // (and not-spanned) MIME type expressions.
    //
    
    /**
     * Tests that <code>A spans B</code> when <code>A == B</code>.
     */

    public void test_spans_when_A_equals_B()
    {

	doSpansTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/xml" ),
	      true		// compare parameters.
	      );

    }

    /**
     * Tests that <code>A spans B</code> and <code>B spans A</code>
     * when <code>A == B</code> and A and B both specify the same MIME
     * type parameters (same MIME type expressions).
     */

    public void test_spans_when_A_equals_B_same_params()
    {

	doSpansTest
	    ( new MIMEType( "text/xml;a=a" ),
	      new MIMEType( "text/xml;a=a" ),
	      true		// compare parameters.
	      );

    }

    /**
     * Tests that the spans implementation correctly ignores the
     * specified MIME type parameters when we ask it to.
     */

    public void test_spans_will_ingore_parameters_when_asked_to()
    {

	doSpansTest
	    ( new MIMEType( "text/xml;a=a" ),
	      new MIMEType( "text/xml;b=a" ),
	      false		// compare parameters.
	      );

    }

    /**
     * Tests that <code>A spans B</code> and that <code>B does not
     * span A</code> when B has same type and subtype but also
     * specifies one or more parameters which are not included in A.
     */

    public void test_spans_A_equals_B_with_extra_params_on_B()
    {

	doSpansTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/xml;a=a" ),
	      true
	      );

	doDoesNotSpanTest
	    ( new MIMEType( "text/xml;a=a" ),
	      new MIMEType( "text/xml" ),
	      true
	      );

    }

    /**
     * Tests that a subtype wildcard spans any specific subtype.
     */

    public void test_spans_subtype_1()
    {

	doSpansTest		// no params.
	    ( new MIMEType( "text/*" ),
	      new MIMEType( "text/xml" ),
	      true
	      );

    }

    public void test_spans_subtype_2()
    {

	doSpansTest		// same params.
	    ( new MIMEType( "text/*;a=a" ),
	      new MIMEType( "text/xml;a=a" ),
	      true
	      );
    }

    public void test_spans_subtype_3()
    {

	doDoesNotSpanTest	// too specific.
	    ( new MIMEType( "text/*;a=a;b=a" ),
	      new MIMEType( "text/xml;a=a" ),
	      true
	      );

    }

    /**
     * This test series is based on an example in RFC 1616, section
     * 14.1, page 63.  The example is meant to demonstrate the meaning
     * of increasing specificity (or increasing generality) for
     * media-ranges and is presented in conjunction with the section
     * on the <code>Accept</code> header.<p>
     *
     * <blockquote>
     *
     * Media ranges can be overridden by more specific media ranges or
     * specific media types. If more than one media range applies to a
     * given type, the most specific reference has precedence. For
     * example,<p>

     <pre>
     
     Accept: text/*, text/html, text/html;level=1, * / *
	
     </pre>

     have the following precedence:<p>

     <pre>

     1) text/html;level=1
     2) text/html
     3) text/*
     4) * / *

     </pre>

    */

    public void test_precedence_order_1()
    {
	
	doSpansTest
	    ( new MIMEType( "text/html" ),
	      new MIMEType( "text/html;level=1" ),
	      true
	      );

	doDoesNotSpanTest
	    ( new MIMEType( "text/html;level=1" ),
	      new MIMEType( "text/html" ),
	      true
	      );

    }

    public void test_precedence_order_2()
    {
	
	doSpansTest
	    ( new MIMEType( "text/*" ),
	      new MIMEType( "text/html;level=1" ),
	      true
	      );

	doDoesNotSpanTest
	    ( new MIMEType( "text/html;level=1" ),
	      new MIMEType( "text/*" ),
	      true
	      );

    }

    public void test_precedence_order_3()
    {
	
	doSpansTest
	    ( new MIMEType( "*/*" ),
	      new MIMEType( "text/*" ),
	      true
	      );

	doDoesNotSpanTest
	    ( new MIMEType( "text/*" ),
	      new MIMEType( "*/*" ),
	      true
	      );

    }


    //************************************************************
    //******************* Intersection ***************************
    //************************************************************

    public void test_intersection_1()
    {

	doIntersectionTest
	    ( new MIMEType( "*/*" ),
	      new MIMEType( "*/*" ),
	      new MIMEType( "*/*" )
	      );

    }

    public void test_intersection_2()
    {

	doIntersectionTest
	    ( new MIMEType( "text/*" ),
	      new MIMEType( "text/*" ),
	      new MIMEType( "text/*" )
	      );

    }

    public void test_intersection_3()
    {

	doIntersectionTest
	    ( new MIMEType( "*/*;charset=US-ASCII" ),
	      new MIMEType( "*/*;charset=US-ASCII" ),
	      new MIMEType( "*/*;charset=US-ASCII" )
	      );

    }

    public void test_intersection_4()
    {

	doIntersectionTest
	    ( new MIMEType( "text/*;charset=US-ASCII" ),
	      new MIMEType( "text/*" ),
	      new MIMEType( "text/*;charset=US-ASCII" )
	      );

    }

    public void test_intersection_5()
    {

	doIntersectionTest
	    ( new MIMEType( "text/*" ),
	      new MIMEType( "text/*;charset=US-ASCII" ),
	      new MIMEType( "text/*;charset=US-ASCII" )
	      );

    }

    public void test_intersection_6()
    {

	doIntersectionTest
	    ( new MIMEType( "text/*;charset=US-ASCII" ),
	      new MIMEType( "text/xml" ),
	      new MIMEType( "text/xml;charset=US-ASCII" )
	      );

    }

    public void test_intersection_7()
    {

	doIntersectionTest
	    ( new MIMEType( "text/xml;lang=en-us" ),
	      new MIMEType( "text/*;charset=US-ASCII" ),
	      new MIMEType( "text/xml;lang=en-us;charset=US-ASCII" )
	      );

    }

    public void test_no_intersection_1()
    {

	doIntersectionTest
	    ( new MIMEType( "application/*" ),
	      new MIMEType( "text/*" ),
	      null
	      );

    }

    public void test_no_intersection_2()
    {

	doIntersectionTest
	    ( new MIMEType( "text/xml" ),
	      new MIMEType( "text/text" ),
	      null
	      );

    }

    public void test_no_intersection_3()
    {

	doIntersectionTest
	    ( new MIMEType( "text/xml;lang=en-us" ),
	      new MIMEType( "text/text;charset=US-ASCII" ),
	      null
	      );

    }

    public static void doIntersectionTest
	( MIMEType a,
	  MIMEType b,
	  MIMEType expectedResult
	  )
    {

	MIMEType actualResult = a.getIntersection
	    ( b,
	      new String[]{}
	      );

	if( expectedResult == null ) {
	    
	    if( actualResult == null ) {
		
		return;
		
	    }

	    fail( "Expecting null intersection, but found"+
		  ": \""+actualResult+"\""
		  );

	}

	if( actualResult == null ) {

	    fail( "Expecting "+expectedResult+", not a null intersection."
		  );

	}

	assertTrue
	    ( "The intersection is correct",
	      expectedResult.isExactMatch
	      ( actualResult
		)
	      );

    }

}
