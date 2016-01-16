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
/*
 * Created on Oct 13, 2011
 */

package com.bigdata.rdf.sail.webapp.client;

import junit.framework.TestCase2;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

/**
 * Test suite for utility class to encode and decode RDF Values for interchange
 * via the REST API.
 * 
 * @see EncodeDecodeValue
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestEncodeDecodeValue extends TestCase2 {

   private static final Logger log = Logger.getLogger(TestEncodeDecodeValue.class);
   
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

   /*
    * Test suite for encode/decode of openrdf style contexts. 
    */
    
    /**
    * Test case for <code>foo(s,p,o,(Resource[]) null)</code>. This is case is
    * disallowed.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
    public void test_encodeDecodeContexts_quads_context_null_array() {

       // rejected by encode.
       try {
          EncodeDecodeValue.encodeContexts(null);
          fail("Expecting " + IllegalArgumentException.class);
       } catch (IllegalArgumentException ex) {
          if (log.isInfoEnabled())
             log.info("Ignoring expected exception: " + ex);
       }

       // rejected by decode.
       try {
          EncodeDecodeValue.decodeContexts(null);
          fail("Expecting " + IllegalArgumentException.class);
       } catch (IllegalArgumentException ex) {
          if (log.isInfoEnabled())
             log.info("Ignoring expected exception: " + ex);
       }

   }

   /**
    * Test case for <code>foo(s,p,o)</code>. This is case is equivalent to
    * <code>foo(s,p,o,new Resource[0]). It is allowed and refers
    * to all named graphs.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_not_specified() {

      final String[] encoded = EncodeDecodeValue
            .encodeContexts(new Resource[0]);

      assertEquals(new String[0], encoded);

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[0], decoded);

   }

   /**
    * Test case for <code>foo(s,p,o,(Resource)null)</code>. This is case is
    * equivalent to
    * <code>foo(s,p,o,new Resource[]{null}). It is allowed and refers
    * to the "null" graph.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_nullGraph() {

      final String[] encoded = EncodeDecodeValue
            .encodeContexts(new Resource[] { null });

      assertEquals(new String[] { null }, encoded);

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[] { null }, decoded);

   }

   /**
    * Test case for <code>foo(s,p,o,(Resource)null)</code>. This is case is
    * equivalent to
    * <code>foo(s,p,o,new Resource[]{null}). It is allowed and refers
    * to the "null" graph.
    * <p>
    * Note: When a <code>null</code> is encoded into an HTTP parameter it will
    * be represented as <code>c=</code>. On decode, the value will be a zero
    * length string. Therefore this test verifies that we decode a zero length
    * string into a <code>null</code>.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_nullGraph_2() {

      final String[] encoded = new String[] { "" };

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[] { null }, decoded);

   }

   /**
    * Test case for <code>foo(s,p,o,x,y,z)</code>. This is case is equivalent to
    * <code>foo(s,p,o,new Resource[]{x,y,z}). It is allowed and refers
    * to the specified named graphs.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_x_y_z() {

      final ValueFactory vf = new ValueFactoryImpl();
      final URI x = vf.createURI(":x");
      final URI y = vf.createURI(":y");
      final URI z = vf.createURI(":z");

      final String[] encoded = EncodeDecodeValue.encodeContexts(new Resource[] {
            x, y, z });

      // Note: Encoding wraps URI with <...>
      assertEquals(new String[] { "<:x>", "<:y>", "<:z>" }, encoded);

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[] { x, y, z }, decoded);

   }

   /**
    * Test case for <code>foo(s,p,o,x,null,z)</code>. This is case is equivalent
    * to <code>foo(s,p,o,new Resource[]{x,null,z}). It is allowed and refers
    * to the named graph (x), the null graph, and the named graph (z).
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_x_null_z() {

      final ValueFactory vf = new ValueFactoryImpl();
      final URI x = vf.createURI(":x");
      final URI z = vf.createURI(":z");

      final String[] encoded = EncodeDecodeValue.encodeContexts(new Resource[] {
            x, null, z });

      // Note: Encoding wraps URI with <...>
      assertEquals(new String[] { "<:x>", null, "<:z>" }, encoded);

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[] { x, null, z }, decoded);

   }


   /**
    * Test case for <code>foo(s,p,o,x,null,z)</code>. This is case is equivalent
    * to <code>foo(s,p,o,new Resource[]{x,null,z}). It is allowed and refers
    * to the named graph (x), the null graph, and the named graph (z).
    * <p>
    * Note: When a <code>null</code> is encoded into an HTTP parameter it will
    * be represented as <code>c=</code>. On decode, the value will be a zero
    * length string. Therefore this test verifies that we decode a zero length
    * string into a <code>null</code>.
    * 
    * @see com.bigdata.rdf.store.BD#NULL_GRAPH
    * @see <a href="http://trac.bigdata.com/ticket/1177"> Resource... contexts
    *      not encoded/decoded according to openrdf semantics (REST API) </a>
    */
   public void test_encodeDecodeContexts_quads_context_x_null_z_2() {

      final ValueFactory vf = new ValueFactoryImpl();
      final URI x = vf.createURI(":x");
      final URI z = vf.createURI(":z");

      // Note: Encoding wraps URI with <...>
      final String[] encoded = new String[] { "<:x>", "", "<:z>" };

      final Resource[] decoded = EncodeDecodeValue.decodeContexts(encoded);

      assertEquals(new Resource[] { x, null, z }, decoded);

   }

}
