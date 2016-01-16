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
 * Created on Jun 30, 2011
 */

package com.bigdata.rdf.lexicon;

import java.util.Locale;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.BytesUtil.UnsignedByteArrayComparator;

/**
 * Test suite for {@link LexiconKeyBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLexiconKeyBuilder extends TestCase2 {

    /**
     * 
     */
    public TestLexiconKeyBuilder() {
    }

    /**
     * @param name
     */
    public TestLexiconKeyBuilder(String name) {
        super(name);
    }

    private LexiconKeyBuilder fixture = null;

    /**
     * {@inheritDoc}
     * <p>
     * Note: The {@link LexiconKeyBuilder} will wind up configured with the
     * default {@link Locale} unless that gets overridden by
     * {@link #getProperties()}
     */
    protected void setUp() throws Exception {

        super.setUp();
        
        final IKeyBuilderFactory keyBuilderFactory = new DefaultKeyBuilderFactory(
                getProperties());

        final IKeyBuilder keyBuilder = keyBuilderFactory.getKeyBuilder();

        fixture = new LexiconKeyBuilder(keyBuilder);

    }
    
    protected void tearDown() throws Exception {
        
        fixture = null;
        
        super.tearDown();
        
    }
    
    /**
     * Tests encode of a key and the decode of its "code" byte.
     * 
     * @see ITermIndexCodes
     */
    public void test_encodeDecodeCodeByte() {
        
        assertEquals(ITermIndexCodes.TERM_CODE_URI, fixture
                .value2Key(RDF.TYPE)[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_BND, fixture
                .value2Key(new BNodeImpl("foo"))[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_LIT, fixture
                .value2Key(new LiteralImpl("abc"))[0]);
        
        assertEquals(ITermIndexCodes.TERM_CODE_LCL, fixture
                .value2Key(new LiteralImpl("abc","en"))[0]);        
        
        assertEquals(ITermIndexCodes.TERM_CODE_DTL, fixture
                .value2Key(new LiteralImpl("abc",XSD.BOOLEAN))[0]);        
        
    }

    /**
     * Tests the gross ordering over the different kinds of {@link Value}s but
     * deliberately does not pay attention to the sort key ordering for string
     * data.
     * 
     * @see ITermIndexCodes
     */
    public void test_keyOrder() {

        final byte[] uri = fixture.value2Key(RDF.TYPE);

        final byte[] bnd = fixture.value2Key(new BNodeImpl("foo"));

        final byte[] lit = fixture.value2Key(new LiteralImpl("abc"));

        final byte[] lcl = fixture.value2Key(new LiteralImpl("abc", "en"));

        final byte[] dtl = fixture.value2Key(new LiteralImpl("abc",
                XSD.BOOLEAN));

        // URIs before plain literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(uri, lit) < 0);

        // plain literals before language code literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(lit, lcl) < 0);

        // language code literals before datatype literals.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(lcl, dtl) < 0);

        // datatype literals before blank nodes.
        assertTrue(UnsignedByteArrayComparator.INSTANCE.compare(dtl, bnd) < 0);

    }
    
    public void test_uri() {
        
        final String uri1 = "http://www.cognitiveweb.org";
        final String uri2 = "http://www.cognitiveweb.org/a";
        final String uri3 = "http://www.cognitiveweb.com/a";
        
        final byte[] k1 = fixture.uri2key(uri1);
        final byte[] k2 = fixture.uri2key(uri2);
        final byte[] k3 = fixture.uri2key(uri3);

        if (log.isInfoEnabled()) {
            log.info("k1(" + uri1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(" + uri2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(" + uri3 + ") = " + BytesUtil.toString(k3));
        }
        
        // subdirectory sorts after root directory.
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        
        // .com extension sorts before .org
        assertTrue(BytesUtil.compareBytes(k2, k3)>0);
        
    }
    
    public void test_plainLiteral() {

        final String lit1 = "abc";
        final String lit2 = "abcd";
        final String lit3 = "abcde";
        
        final byte[] k1 = fixture.plainLiteral2key(lit1);
        final byte[] k2 = fixture.plainLiteral2key(lit2);
        final byte[] k3 = fixture.plainLiteral2key(lit3);

        if (log.isInfoEnabled()) {
            log.info("k1(" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(" + lit3 + ") = " + BytesUtil.toString(k3));
        }
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    public void test_languageCodeLiteral() {
        
        final String en = "en";
        final String de = "de";
        
        final String lit1 = "abc";
        final String lit2 = "abc";
        final String lit3 = "abce";
        
        final byte[] k1 = fixture.languageCodeLiteral2key(en, lit1);
        final byte[] k2 = fixture.languageCodeLiteral2key(de, lit2);
        final byte[] k3 = fixture.languageCodeLiteral2key(de, lit3);

        if (log.isInfoEnabled()) {
            log.info("k1(en:" + lit1 + ") = " + BytesUtil.toString(k1));
            log.info("k2(de:" + lit2 + ") = " + BytesUtil.toString(k2));
            log.info("k3(de:" + lit3 + ") = " + BytesUtil.toString(k3));
        }
        
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
        
        final String id1 = "_12";
        final String id2 = "_abc";
        final String id3 = "abc";
        
        final byte[] k1 = fixture.blankNode2Key(id1);
        final byte[] k2 = fixture.blankNode2Key(id2);
        final byte[] k3 = fixture.blankNode2Key(id3);

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
        
        final byte[] k1 = fixture.uri2key("http://www.cognitiveweb.org");
        final byte[] k2 = fixture.plainLiteral2key("hello world!");
        final byte[] k3 = fixture.blankNode2Key("a12");
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);
        
    }
    
    /**
     * This is an odd issue someone reported for the trunk. There are two
     * version of a plain Literal <code>Brian McCarthy</code>, but it appears
     * that one of the two versions has a leading bell character when you decode
     * the Unicode byte[]. I think that this is actually an issue with the
     * {@link Locale} and the Unicode sort key generation. If {@link KeyBuilder}
     * as configured on the system generates Unicode sort keys which compare as
     * EQUAL for these two inputs then that will cause the lexicon to report an
     * "apparent" inconsistency. In fact, what we probably need to do is just
     * disable the inconsistency check in the lexicon.
     * 
     * <pre>
     * ERROR: com.bigdata.rdf.lexicon.Id2TermWriteProc.apply(Id2TermWriteProc.java:205): val=[0, 2, 0, 14, 66, 114, 105, 97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121]
     * ERROR: com.bigdata.rdf.lexicon.Id2TermWriteProc.apply(Id2TermWriteProc.java:206): oldval=[0, 2, 0, 15, 127, 66, 114, 105, 97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121]
     * </pre>
     */
    public void test_consistencyIssue() {

        final BigdataValueSerializer<Value> fixture = new BigdataValueSerializer<Value>(
                ValueFactoryImpl.getInstance());

        final byte[] newValBytes = new byte[] { 0, 2, 0, 14, 66, 114, 105, 97, 110, 32,
                77, 99, 67, 97, 114, 116, 104, 121 };

        final byte[] oldValBytes = new byte[] { 0, 2, 0, 15, 127, 66, 114, 105,
                97, 110, 32, 77, 99, 67, 97, 114, 116, 104, 121 };

        final Value newValue = fixture.deserialize(newValBytes);

        final Value oldValue = fixture.deserialize(oldValBytes);

        if (log.isInfoEnabled()) {
            log.info("new=" + newValue);
            log.info("old=" + oldValue);
        }

        /*
         * Note: This uses the default Locale and the implied Unicode collation
         * order to generate the sort keys.
         */
//        final IKeyBuilder keyBuilder = new KeyBuilder();

        /*
         * Note: This allows you to explicitly configure the behavior of the
         * KeyBuilder instance based on the specified properties.  If you want
         * your KB to run with these properties, then you need to specify them
         * either in your environment or using -D to java.
         */
        final Properties properties = new Properties();
        
        // specify that all aspects of the Unicode sequence are significant.
        properties.setProperty(KeyBuilder.Options.STRENGTH,StrengthEnum.Identical.toString());
        
//        // specify that that only primary character differences are significant.
//        properties.setProperty(KeyBuilder.Options.STRENGTH,StrengthEnum.Primary.toString());
        
        final IKeyBuilder keyBuilder = KeyBuilder
                .newUnicodeInstance(properties);

        final LexiconKeyBuilder lexKeyBuilder = new LexiconKeyBuilder(
                keyBuilder);

        // encode as unsigned byte[] key.
        final byte[] newValKey = lexKeyBuilder.value2Key(newValue);

        final byte[] oldValKey = lexKeyBuilder.value2Key(oldValue);

        if (log.isInfoEnabled()) {
            log.info("newValKey=" + BytesUtil.toString(newValKey));
            log.info("oldValKey=" + BytesUtil.toString(oldValKey));
        }

        /*
         * Note: if this assert fails then the two distinct Literals were mapped
         * onto the same unsigned byte[] key.
         */
        assertFalse(BytesUtil.bytesEqual(newValKey, oldValKey));

    }

}
