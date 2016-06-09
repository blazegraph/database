/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.internal;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.rdf.lexicon.TestTermIVComparator;
import com.bigdata.rdf.store.TestLocalTripleStore;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("RDF Internal Values");

        // test suite for the DTEFlags (bit patterns).
        suite.addTestSuite(TestDTEFlags.class);
        
        // test suite for VTE.
        suite.addTestSuite(TestVTE.class);
        
        // test suite for DTE.
        suite.addTestSuite(TestDTE.class);

        // basic test suite for TermIV.
        suite.addTestSuite(TestTermIV.class);

        // test suite for putting BigdataValues in TermIV order.
        suite.addTestSuite(TestTermIVComparator.class);

        // basic test suite for BlobIV.
        suite.addTestSuite(TestBlobIV.class);

        // unit tests for fully inline literals.
        suite.addTestSuite(TestFullyInlineTypedLiteralIV.class);

        // unit tests for fully inline URIs.
        suite.addTestSuite(TestFullyInlineURIIV.class);

        // unit tests for fully inline URIs based on a namespace + localName.
        suite.addTestSuite(TestURIExtensionIV.class);

        // unit tests for inline literals with a datatype IV.
        suite.addTestSuite(TestLiteralDatatypeIV.class);

        // test suite for encode/decode of IVs.
        suite.addTestSuite(TestEncodeDecodeKeys.class);

        // test suite for encode/decode of xsd:integer IVs
        suite.addTestSuite(TestEncodeDecodeXSDIntegerIVs.class);

        // test suite for encode/decode of GeoSpatial literals
        suite.addTestSuite(TestEncodeDecodeGeoSpatialLiteralIVs.class);
        
        // test suite for encode/decode of date time literals
        suite.addTestSuite(TestEncodeDecodeXSDDateIVs.class);
        
        // test suite for GeoSpatial utility
        suite.addTestSuite(TestZOrderRangeScanUtility.class);

        // test suite for encode/decode of xsd:decimal IVs
        suite.addTestSuite(TestEncodeDecodeXSDDecimalIVs.class);

        /*
         * Test suite for encode/decode of IVs which inline Unicode data.
         * 
         * Note: All of these tests currently fail. The failures appear to be
         * related to pretty much the same cause in each case. While I have not
         * tracked down the cause, it appears to be related to the choice of the
         * various short strings and their ordering by Java#toString() versus
         * the encoded Unicode data.  I have filed an issue to support inlining
         * unicode data.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/334 
         */
        suite.addTestSuite(TestIVUnicode.class);
        suite.addTestSuite(TestEncodeDecodeUnicodeIVs.class);

        /*
         * Test suite for inlining of xsd unsigned data types.
         * 
         * Note: This feature is not currently supported.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/246
         */
        suite.addTestSuite(TestUnsignedIVs.class);
        suite.addTestSuite(TestUnsignedIntegerIVs.class);
        
        // Encode/decode and *comparator* for mixed VIs.
        suite.addTestSuite(TestEncodeDecodeMixedIVs.class);

        // Encoding/decoding of individual IV binding sets
        suite.addTest(com.bigdata.rdf.internal.encoder.TestAll.suite());

        // inline URI tests.
        suite.addTest(com.bigdata.rdf.internal.impl.uri.TestAll.suite());

        /*
         * Note: This is an old and never finished test suite. All it does is
         * explore some of the available hash functions having more than 32 bits
         * in the generated hash code. However, it seems like 32-bits is plenty.
         */
        // suite.addTestSuite(TestLongLiterals.class);

        // xpath abs(), ceil(), floor(), and round()
        suite.addTestSuite(TestXPathFunctions.class);

        // geospatial format handling.
        suite.addTest(com.bigdata.rdf.internal.gis.TestAll.suite());

        // DTEExtension encoding of packed long integer representing a timestamp.
        suite.addTestSuite(TestEncodeDecodePackedLongIVs.class);
        suite.addTestSuite(TestPackedLongIVs.class);
        
        // DTEExtension.IPV4
        suite.addTestSuite(TestEncodeDecodeIPv4AddrIV.class);
        
        // DTEExtension.ARRAY
        suite.addTestSuite(TestEncodeDecodeLiteralArrayIVs.class);
        
        // DTEExtension.IPV4
        suite.addTestSuite(TestEncodeDecodeIPv4AddrIV.class);
        
        // DTEExtension.ARRAY
        suite.addTestSuite(TestEncodeDecodeLiteralArrayIVs.class);
        
        //Inline URI Handlers
        suite.addTestSuite(TestInlineURIHandlers.class);
        
        //Test handlers for packing multiple inline URI handlers into a single
        //namespace.
        suite.addTestSuite(TestInlineLocalNameIntegerURIHandler.class);
        
        //Test Handler for single namespace multiple inline URI Handlers
        //BLZG-1938
       
        final ProxyTestSuite proxySuite = new ProxyTestSuite(new TestLocalTripleStore(),
                "Local Triple Store With Provenance Test Suite"); 
        
        proxySuite.addTest(TestMultiInlineURIHandlersSingleNamespace.suite());
        
        suite.addTest( proxySuite);
       
        return suite;
        
    }
    
}
