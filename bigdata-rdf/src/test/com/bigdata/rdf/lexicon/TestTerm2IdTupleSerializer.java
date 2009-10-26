/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jan 18, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Locale;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.lexicon.LexiconKeyBuilder.XSDBooleanCoder;

/**
 * Test suite for construction of variable length unsigned byte[] keys from RDF
 * {@link Value}s and statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTerm2IdTupleSerializer extends TestCase2 {

    /**
     * Note: The key builder will wind up configured with the default
     * {@link Locale} unless that gets overridden by {@link #getProperties()}
     */
    final Term2IdTupleSerializer tupleSer = new Term2IdTupleSerializer(
            new DefaultKeyBuilderFactory(getProperties()));
    
    final LexiconKeyBuilder fixture = tupleSer.getLexiconKeyBuilder();
    
    /**
     * 
     */
    public TestTerm2IdTupleSerializer() {
    }

    /**
     * @param name
     */
    public TestTerm2IdTupleSerializer(String name) {
        super(name);
    }

    public void test_uri() {
        
        String uri1 = "http://www.cognitiveweb.org";
        String uri2 = "http://www.cognitiveweb.org/a";
        String uri3 = "http://www.cognitiveweb.com/a";
        
        byte[] k1 = fixture.uri2key(uri1);
        byte[] k2 = fixture.uri2key(uri2);
        byte[] k3 = fixture.uri2key(uri3);

        System.err.println("k1("+uri1+") = "+BytesUtil.toString(k1));
        System.err.println("k2("+uri2+") = "+BytesUtil.toString(k2));
        System.err.println("k3("+uri3+") = "+BytesUtil.toString(k3));
        
        // subdirectory sorts after root directory.
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        
        // .com extension sorts before .org
        assertTrue(BytesUtil.compareBytes(k2, k3)>0);
        
    }
    
    public void test_plainLiteral() {

        String lit1 = "abc";
        String lit2 = "abcd";
        String lit3 = "abcde";
        
        byte[] k1 = fixture.plainLiteral2key(lit1);
        byte[] k2 = fixture.plainLiteral2key(lit2);
        byte[] k3 = fixture.plainLiteral2key(lit3);

        System.err.println("k1("+lit1+") = "+BytesUtil.toString(k1));
        System.err.println("k2("+lit2+") = "+BytesUtil.toString(k2));
        System.err.println("k3("+lit3+") = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_languageCodeLiteral() {
        
        String en = "en";
        String de = "de";
        
        String lit1 = "abc";
        String lit2 = "abc";
        String lit3 = "abce";
        
        byte[] k1 = fixture.languageCodeLiteral2key(en, lit1);
        byte[] k2 = fixture.languageCodeLiteral2key(de, lit2);
        byte[] k3 = fixture.languageCodeLiteral2key(de, lit3);

        System.err.println("k1(en:"+lit1+") = "+BytesUtil.toString(k1));
        System.err.println("k2(de:"+lit2+") = "+BytesUtil.toString(k2));
        System.err.println("k3(de:"+lit3+") = "+BytesUtil.toString(k3));
        
        // "en" sorts after "de".
        assertTrue(BytesUtil.compareBytes(k1, k2)>0);

        // en:abc != de:abc
        assertTrue(BytesUtil.compareBytes(k1, k2) != 0);
        
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_plain_vs_languageCode_literal() {
        
        final String en = "en";
//        String de = "de";
        
        final String lit1 = "abc";
//        String lit2 = "abc";
//        String lit3 = "abce";
//        final Literal a = new LiteralImpl("foo");
//        final Literal b = new LiteralImpl("foo", "en");

        final byte[] k1 = fixture.plainLiteral2key(lit1);
        final byte[] k2 = fixture.languageCodeLiteral2key(en, lit1);
        
        // not encoded onto the same key.
        assertFalse(BytesUtil.bytesEqual(k1, k2));
        
        // the plain literals are ordered before the language code literals.
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        
    }

    /**
     * Verify an unknown datatype URI is coded.
     */
    public void test_datatype_unknown() {

        fixture.datatypeLiteral2key(new URIImpl("http://www.bigdata.com/foo"),
                "foo");
        
    }
    
    public void test_datatypeLiteral_xsd_boolean() {
        
        final URI datatype = XMLSchema.BOOLEAN;
        
        final String lit1 = "true";
        final String lit2 = "false";
        final String lit3 = "1";
        final String lit4 = "0";
        
        final byte[] k1 = fixture.datatypeLiteral2key(datatype,lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(datatype,lit2);
        final byte[] k3 = fixture.datatypeLiteral2key(datatype,lit3);
        final byte[] k4 = fixture.datatypeLiteral2key(datatype,lit4);

        if (log.isInfoEnabled()) {
            log.info("k1(boolean:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(boolean:" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(boolean:" + lit3 + ") = " + BytesUtil.toString(k3));
            log.info("k4(boolean:" + lit4 + ") = " + BytesUtil.toString(k4));
        }
        
        assertTrue(BytesUtil.compareBytes(k1, k2) != 0);
        assertTrue(BytesUtil.compareBytes(k1, k2) > 0);

        /*
         * Note: if we do not normalize data type values then these are
         * inequalities.
         */
        assertTrue(BytesUtil.compareBytes(k1, k3) != 0); // true != 1
        assertTrue(BytesUtil.compareBytes(k2, k4) != 0); // false != 0

        // verify decode.
        final KeyBuilder keyBuilder = new KeyBuilder();
        // decode(encode(true))
        XSDBooleanCoder.INSTANCE.encode(keyBuilder.reset(), "true");
        assertEquals("true", XSDBooleanCoder.INSTANCE.decode(keyBuilder
                .getBuffer(), 0, 1));
        // decode(encode(false))
        XSDBooleanCoder.INSTANCE.encode(keyBuilder.reset(), "false");
        assertEquals("false", XSDBooleanCoder.INSTANCE.decode(keyBuilder
                .getBuffer(), 0, 1));

    }
    
    public void test_datatypeLiteral_xsd_int() {
        
        final URI datatype = XMLSchema.INT;
        
        // Note: leading zeros are ignored in the xsd:int value space.
        final String lit1 = "-4";
        final String lit2 = "005";
        final String lit3 = "5";
        final String lit4 = "6";
        
        final byte[] k1 = fixture.datatypeLiteral2key(datatype,lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(datatype,lit2);
        final byte[] k3 = fixture.datatypeLiteral2key(datatype,lit3);
        final byte[] k4 = fixture.datatypeLiteral2key(datatype,lit4);

        if (log.isInfoEnabled()) {
            log.info("k1(int:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(int:" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k2(int:" + lit3 + ") = " + BytesUtil.toString(k3));
            log.info("k4(int:" + lit4 + ") = " + BytesUtil.toString(k4));
        }
        
        assertTrue(BytesUtil.compareBytes(k1, k2) < 0);
        assertTrue(BytesUtil.compareBytes(k3, k4) < 0);

        /*
         * Note: if we do not normalize data type values then these are
         * inequalities.
         */
        assertTrue(BytesUtil.compareBytes(k2, k3) != 0); // 005 != 5

    }

    /**
     * Verify that the value spaces for long, int, short and byte are disjoint.
     */
    public void test_disjoint_value_space() {
        
        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.LONG, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.INT, "-1")//
                ));

        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.LONG, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.SHORT, "-1")//
                ));
        
        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.LONG, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.BYTE, "-1")//
                ));

        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.INT, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.SHORT, "-1")//
                ));
        
        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.INT, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.BYTE, "-1")//
                ));

        assertFalse(BytesUtil.bytesEqual(//
                fixture.datatypeLiteral2key(XMLSchema.SHORT, "-1"),//
                fixture.datatypeLiteral2key(XMLSchema.BYTE, "-1")//
                ));

    }
    
    public void test_datatypeLiteral_xsd_float() {
        
        final URI datatype = XMLSchema.FLOAT;
        
        // Note: leading zeros are ignored in the xsd:int value space.
        final String lit1 = "-4.0";
        final String lit2 = "005";
        final String lit3 = "5.";
        final String lit4 = "5.0";
        final String lit5 = "6";
        
        final byte[] k1 = fixture.datatypeLiteral2key(datatype,lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(datatype,lit2);
        final byte[] k3 = fixture.datatypeLiteral2key(datatype,lit3);
        final byte[] k4 = fixture.datatypeLiteral2key(datatype,lit4);
        final byte[] k5 = fixture.datatypeLiteral2key(datatype,lit5);

        if (log.isInfoEnabled()) {
            log.info("k1(float:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(float:" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(float:" + lit3 + ") = " + BytesUtil.toString(k3));
            log.info("k4(float:" + lit3 + ") = " + BytesUtil.toString(k4));
            log.info("k5(float:" + lit5 + ") = " + BytesUtil.toString(k5));
        }

        assertTrue(BytesUtil.compareBytes(k1, k2) < 0);
        assertTrue(BytesUtil.compareBytes(k4, k5) < 0);

        /*
         * Note: if we do not normalize data type values then these are
         * inequalities.
         */
        assertTrue(BytesUtil.compareBytes(k2, k3) != 0); // 005 != 5.
        assertTrue(BytesUtil.compareBytes(k3, k4) != 0); // 5. != 5.0

    }
    
    public void test_datatypeLiteral_xsd_double() {
        
        final URI datatype = XMLSchema.DOUBLE;
        
        // Note: leading zeros are ignored in the xsd:int value space.
        final String lit1 = "-4.0";
        final String lit2 = "005";
        final String lit3 = "5.";
        final String lit4 = "5.0";
        final String lit5 = "6";
        
        final byte[] k1 = fixture.datatypeLiteral2key(datatype,lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(datatype,lit2);
        final byte[] k3 = fixture.datatypeLiteral2key(datatype,lit3);
        final byte[] k4 = fixture.datatypeLiteral2key(datatype,lit4);
        final byte[] k5 = fixture.datatypeLiteral2key(datatype,lit5);

        if (log.isInfoEnabled()) {
            log.info("k1(double:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(double:" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(double:" + lit3 + ") = " + BytesUtil.toString(k3));
            log.info("k4(double:" + lit3 + ") = " + BytesUtil.toString(k4));
            log.info("k5(double:" + lit5 + ") = " + BytesUtil.toString(k5));
        }

        assertTrue(BytesUtil.compareBytes(k1, k2) < 0);
        assertTrue(BytesUtil.compareBytes(k4, k5) < 0);

        /*
         * Note: if we do not normalize data type values then these are
         * inequalities.
         */
        assertTrue(BytesUtil.compareBytes(k2, k3) != 0); // 005 != 5.
        assertTrue(BytesUtil.compareBytes(k3, k4) != 0); // 5. != 5.0

    }

    /**
     * Verify that some value spaces are disjoint.
     */
    public void test_datatypeLiteral_xsd_int_not_double_or_float() {
        
        final String lit1 = "4";
        
        final byte[] k0 = fixture.datatypeLiteral2key(XMLSchema.INT, lit1);
        final byte[] k1 = fixture.datatypeLiteral2key(XMLSchema.FLOAT, lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(XMLSchema.DOUBLE, lit1);

        if (log.isInfoEnabled()) {
            log.info("k0(float:" + lit1 + ") = " + BytesUtil.toString(k0));
            log.info("k1(float:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(double:" + lit1 + ") = " + BytesUtil.toString(k2));
        }

        assertTrue(BytesUtil.compareBytes(k0, k1) != 0);
        assertTrue(BytesUtil.compareBytes(k0, k2) != 0);
        
    }

    /**
     * Verify that some value spaces are disjoint.
     */
    public void test_datatypeLiteral_xsd_float_not_double() {
        
        final String lit1 = "04.21";
        
        final byte[] k1 = fixture.datatypeLiteral2key(XMLSchema.FLOAT,lit1);
        final byte[] k2 = fixture.datatypeLiteral2key(XMLSchema.DOUBLE,lit1);

        if (log.isInfoEnabled()) {
            log.info("k1(float:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(double:" + lit1 + ") = " + BytesUtil.toString(k2));
        }

        assertTrue(BytesUtil.compareBytes(k1, k2) != 0);
        
    }
    
    public void test_blankNode() {
        
        String id1 = "_12";
        String id2 = "_abc";
        String id3 = "abc";
        
        byte[] k1 = fixture.blankNode2Key(id1);
        byte[] k2 = fixture.blankNode2Key(id2);
        byte[] k3 = fixture.blankNode2Key(id3);

        if (log.isInfoEnabled()) {
            log.info("k1(bnodeId:" + id1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(bnodeId:" + id2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(bnodeId:" + id3 + ") = " + BytesUtil.toString(k3));
        }
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }

    /**
     * Test verifies the ordering among URIs, Literals, and BNodes. This
     * ordering is important when batching terms of these different types into
     * the term index since you want to insert the type types according to this
     * order for the best performance.
     */
    public void test_termTypeOrder() {

        /*
         * one key of each type. the specific values for the types do not matter
         * since we are only interested in the relative order between those
         * types in this test.
         */
        
        byte[] k1 = fixture.uri2key("http://www.cognitiveweb.org");
        byte[] k2 = fixture.plainLiteral2key("hello world!");
        byte[] k3 = fixture.blankNode2Key("a12");
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
}
