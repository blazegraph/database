/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct 13, 2011
 */

package com.bigdata.rdf.sail.webapp.client;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

/**
 * Test suite for utility class to encode and decode RDF Values for interchange
 * via the REST API.
 * 
 * @see EncodeDecodeValue
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEncodeDecodeValue extends TestCase2 {

    /**
     * 
     */
    public TestEncodeDecodeValue() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeValue(String name) {
        super(name);
    }

//    public void test_encodeDecode_escapeCodeSequence() {
//
//        assertEquals("\"",EncodeDecodeValue.decodeEscapeSequences("\\u))
//        // doEscapeCodeSequenceTest("","");
//        fail("write tests");
//        
//    }
    
    public void test_encodeDecode_URI() {
        
        doTest("<http://www.bigdata.com>",
                new URIImpl("http://www.bigdata.com"));

        doTest("<" + RDF.TYPE.stringValue() + ">", RDF.TYPE);

        doTest("<http://xmlns.com/foaf/0.1/Person>", new URIImpl(
                "http://xmlns.com/foaf/0.1/Person"));
        
    }

    public void test_encodeDecode_Literal() {
        
        doTest("\"abc\"", new LiteralImpl("abc"));

        doTest("\"\"", new LiteralImpl(""));

        assertEquals(new LiteralImpl("ab\"c"),
                EncodeDecodeValue.decodeValue("\"ab\"c\""));

        assertEquals(new LiteralImpl("ab\"c"),
                EncodeDecodeValue.decodeValue("\"ab\"c\""));

    }
    
    /**
     * Unit tests for a literal which uses single quotes in its encoded form.
     * For example <code>'abc'</code>.
     */
    public void test_encodeDecode_Literal_singleQuotes() {
        
        assertEquals(new LiteralImpl("abc"),
                EncodeDecodeValue.decodeValue("'abc'"));

        assertEquals(new LiteralImpl(""),
                EncodeDecodeValue.decodeValue("''"));

        LiteralImpl l3 = new LiteralImpl("ab\"c");
        Value v3 = EncodeDecodeValue.decodeValue("'ab\"c'");
        assertEquals(l3, v3);

        assertEquals(new LiteralImpl("ab'c"),
                EncodeDecodeValue.decodeValue("'ab\'c'"));

        assertEquals(new LiteralImpl("ab'c"),
                EncodeDecodeValue.decodeValue("'ab'c'"));

        doTest("\"'ab'c'\"", new LiteralImpl("'ab'c'"));
    }

    public void test_encodeDecode_Literal_languageCode() {
        
        doTest("\"abc\"@en", new LiteralImpl("abc","en"));
        
        doTest("\"'ab'c'\"@en", new LiteralImpl("'ab'c'","en"));

        doTest("\"\"@en", new LiteralImpl("","en"));

        doTest("\"\"@", new LiteralImpl("",""));

    }

    public void test_encodeDecode_Literal_datatype() {
        
        doTest("\"abc\"^^<http://www.bigdata.com/dt1>", new LiteralImpl("abc",
                new URIImpl("http://www.bigdata.com/dt1")));

        doTest("\"\"^^<http://www.bigdata.com/dt1>", new LiteralImpl("",
                new URIImpl("http://www.bigdata.com/dt1")));

        doTest("\"'ab'c'\"^^<http://www.bigdata.com/dt1>", new LiteralImpl("'ab\'c'",
                new URIImpl("http://www.bigdata.com/dt1")));

    }

    /**
     * This demonstrates how things could go wrong.
     */
    public void test_encodeDecode_URI_escapeCodeSequence() {
        
        doTest("<http://www.bigdata.com/<>/foo>",
                new URIImpl("http://www.bigdata.com/<>/foo"));
        
         
    }

    /**
     * This demonstrates how things could go wrong. This is a pretty likely
     * scenario and definitely needs to be fixed. The code point for the double
     * quote is 0x0022. The code point for the single quote is 0x0027. The code
     * point for &gt; is 0x003E. The code point for &lt; is 0x003C.
     */
    public void test_encodeDecode_Literal_escapeCodeSequence() {
        
        doTest("\"ab\"c\"", new LiteralImpl("ab\"c"));
        doTest("\"ab'c\"", new LiteralImpl("ab'c"));
               
        doTest("\"ab&c<>\"", new LiteralImpl("ab&c<>"));
                
    }

    /**
     * Round trip encode/decode test.
     * 
     * @param expectedString
     *            The expected external form.
     * @param expectedValue
     *            The given RDF {@link Value}.
     */
    private void doTest(final String expectedString, final Value expectedValue) {

        final String actualString = EncodeDecodeValue
                .encodeValue(expectedValue);

        assertEquals(expectedString, actualString);

        final Value decodedValue = EncodeDecodeValue
                .decodeValue(expectedString);
        
        assertEquals(expectedValue, decodedValue);

        if (log.isInfoEnabled()) {
            log.info("expected=" + expectedString);
            log.info("encoded =" + actualString);
        }

    }

}
