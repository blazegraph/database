/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 19, 2010
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import junit.framework.TestCase2;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.ColorsEnumExtension.Color;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.TermsIndexHelper;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Unit tests for encoding and decoding compound keys (such as are used by the
 * statement indices) in which some of the key components are inline values
 * having variable component lengths while others are term identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2756 2010-05-03 22:26:18Z
 *          thompsonbry$
 * 
 *          FIXME Test heterogenous sets of {@link IV}s, ideally randomly
 *          generated for each type of VTE and DTE.
 */
public class TestEncodeDecodeKeys extends TestCase2 {

    public TestEncodeDecodeKeys() {
        super();
    }
    
    public TestEncodeDecodeKeys(String name) {
        super(name);
    }

	private MockTermIdFactory termIdFactory;
	
	protected void setUp() throws Exception {
		super.setUp();
		termIdFactory = new MockTermIdFactory();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		termIdFactory = null;
	}

    /**
     * Factory for {@link TermId}s.
     */
    private TermId newTermId(final VTE vte) {
        return termIdFactory.newTermId(vte);
    }

    public void test_InlineValue() {

        for (VTE vte : VTE.values()) {

            for (DTE dte : DTE.values()) {

                final IV<?, ?> v = new AbstractIV(vte,
                        true/* inline */, false/* extension */, dte) {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean equals(Object o) {
                        if (this == o)
                            return true;
                        return false;
                    }
                    
                    public int byteLength() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int hashCode() {
                        return 0;
                    }
                    
                    public int compareTo(Object o) {
                        throw new UnsupportedOperationException();
                    }

                    protected int _compareTo(IV o) {
                        throw new UnsupportedOperationException();
                    }

                    public BigdataValue asValue(final LexiconRelation lex)
                            throws UnsupportedOperationException {
                        return null;
                    }

                    public Object getInlineValue()
                            throws UnsupportedOperationException {
                        return null;
                    }

//                    public long getTermId() {
//                        throw new UnsupportedOperationException();
//                    }

                    public boolean isInline() {
                        return true;
                    }

                    public boolean isTermId() {
                        return false;
                    }

                };

                assertFalse(v.isTermId());

                assertTrue(v.isInline());

//                if (termId == 0L) {
//                    assertTrue(v.toString(), v.isNull());
//                } else {
//                    assertFalse(v.toString(), v.isNull());
//                }
//
//                assertEquals(termId, v.getTermId());

                // should not throw an exception.
                v.getInlineValue();

                assertEquals("flags=" + v.flags(), vte, v.getVTE());

                assertEquals(dte, v.getDTE());

                switch (vte) {
                case URI:
                    assertTrue(v.isURI());
                    assertFalse(v.isBNode());
                    assertFalse(v.isLiteral());
                    assertFalse(v.isStatement());
                    break;
                case BNODE:
                    assertFalse(v.isURI());
                    assertTrue(v.isBNode());
                    assertFalse(v.isLiteral());
                    assertFalse(v.isStatement());
                    break;
                case LITERAL:
                    assertFalse(v.isURI());
                    assertFalse(v.isBNode());
                    assertTrue(v.isLiteral());
                    assertFalse(v.isStatement());
                    break;
                case STATEMENT:
                    assertFalse(v.isURI());
                    assertFalse(v.isBNode());
                    assertFalse(v.isLiteral());
                    assertTrue(v.isStatement());
                    break;
                default:
                    fail("vte=" + vte);
                }

            }

        }

    }

    /**
     * Decode a key from one of the statement indices. The components of the key
     * are returned in the order in which they appear in the key. The caller
     * must reorder those components using their knowledge of which index is
     * being decoded in order to reconstruct the corresponding RDF statement.
     * The returned array will always have 4 components. However, the last key
     * component will be <code>null</code> if there are only three components in
     * the <i>key</i>.
     * 
     * @param key
     *            The key.
     * 
     * @return An ordered array of the {@link IV}s for that key.
     */
    static public IV[] decodeStatementKey(final byte[] key, final int arity) {

        return IVUtility.decode(key, arity);
        
    }

    /**
     * Encodes an array of {@link IV}s and then decodes them and
     * verifies that the decoded values are equal-to the original values.
     * 
     * @param e
     *            The array of the expected values.
     */
    static protected IV<?, ?>[] doEncodeDecodeTest(final IV<?, ?>[] e) {

        /*
         * Encode.
         */
        final byte[] key;
        final IKeyBuilder keyBuilder = new KeyBuilder();
        {

            keyBuilder.reset();
            
            for (int i = 0; i < e.length; i++) {

                e[i].encode(keyBuilder);

            }

            key = keyBuilder.getKey();
        }

        /*
         * Decode
         */
        final IV<?, ?>[] a = decodeStatementKey(key, e.length);
        {

            for (int i = 0; i < e.length; i++) {

                final IV expected = e[i];

                final IV actual = a[i];
                
                if (!expected.equals(actual)) {

                    fail("index=" + Integer.toString(i) + " : expected="
                            + expected + ", actual=" + actual);

                }

                if (expected.hashCode() != actual.hashCode()) {

                    fail("index=" + Integer.toString(i) + " : expected="
                            + expected.hashCode() + ", actual="
                            + actual.hashCode());

                }

            }

        }
        
        /*
         * Round-trip serialization.
         */
        {

            for (int i = 0; i < e.length; i++) {

                final IV expected = e[i];

                final byte[] data = SerializerUtil.serialize(expected);
                
                final IV actual = (IV) SerializerUtil.deserialize(data);
                
                if(!expected.equals(actual)) {
                 
                    fail("Round trip serialization problem: expected="
                            + expected + ", actual=" + actual);
                    
                }
                
            }
            
        }

        return a;
        
    }

    /**
     * Unit test for encoding and decoding a statement formed from
     * {@link TermId}s.
     */
    public void test_encodeDecode_allTermIds() {

        final IV<?, ?>[] e = {//
                newTermId(VTE.URI),//
                newTermId(VTE.URI),//
                newTermId(VTE.URI),//
                newTermId(VTE.URI) //
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:boolean.
     */
    public void test_encodeDecode_XSDBoolean() {

        final IV<?, ?>[] e = {//
                new XSDBooleanIV<BigdataLiteral>(true),//
                new XSDBooleanIV<BigdataLiteral>(false),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDByteIV}.
     */
    public void test_encodeDecode_XSDByte() {

        final IV<?, ?>[] e = {//
                new XSDByteIV<BigdataLiteral>((byte)Byte.MIN_VALUE),//
                new XSDByteIV<BigdataLiteral>((byte)-1),//
                new XSDByteIV<BigdataLiteral>((byte)0),//
                new XSDByteIV<BigdataLiteral>((byte)1),//
                new XSDByteIV<BigdataLiteral>((byte)Byte.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDShortIV}.
     */
    public void test_encodeDecode_XSDShort() {

        final IV<?, ?>[] e = {//
                new XSDShortIV<BigdataLiteral>((short)-1),//
                new XSDShortIV<BigdataLiteral>((short)0),//
                new XSDShortIV<BigdataLiteral>((short)1),//
                new XSDShortIV<BigdataLiteral>((short)Short.MIN_VALUE),//
                new XSDShortIV<BigdataLiteral>((short)Short.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDIntIV}.
     */
    public void test_encodeDecode_XSDInt() {

        final IV<?, ?>[] e = {//
                new XSDIntIV<BigdataLiteral>(1),//
                new XSDIntIV<BigdataLiteral>(0),//
                new XSDIntIV<BigdataLiteral>(-1),//
                new XSDIntIV<BigdataLiteral>(Integer.MAX_VALUE),//
                new XSDIntIV<BigdataLiteral>(Integer.MIN_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDLongIV}.
     */
    public void test_encodeDecode_XSDLong() {

        final IV<?, ?>[] e = {//
                new XSDLongIV<BigdataLiteral>(1L),//
                new XSDLongIV<BigdataLiteral>(0L),//
                new XSDLongIV<BigdataLiteral>(-1L),//
                new XSDLongIV<BigdataLiteral>(Long.MIN_VALUE),//
                new XSDLongIV<BigdataLiteral>(Long.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDFloatIV}.
     */
    public void test_encodeDecode_XSDFloat() {

        /*
         * Note: -0f and +0f are converted to the same point in the value space.
         */
//        new XSDFloatIV<BigdataLiteral>(-0f);

        final IV<?, ?>[] e = {//
                new XSDFloatIV<BigdataLiteral>(1f),//
                new XSDFloatIV<BigdataLiteral>(-1f),//
                new XSDFloatIV<BigdataLiteral>(+0f),//
                new XSDFloatIV<BigdataLiteral>(Float.MAX_VALUE),//
                new XSDFloatIV<BigdataLiteral>(Float.MIN_VALUE),//
                new XSDFloatIV<BigdataLiteral>(Float.MIN_NORMAL),//
                new XSDFloatIV<BigdataLiteral>(Float.POSITIVE_INFINITY),//
                new XSDFloatIV<BigdataLiteral>(Float.NEGATIVE_INFINITY),//
                new XSDFloatIV<BigdataLiteral>(Float.NaN),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDDoubleIV}.
     */
    public void test_encodeDecode_XSDDouble() {

        /*
         * Note: -0d and +0d are converted to the same point in the value space.
         */
//      new XSDDoubleIV<BigdataLiteral>(-0d);

        final IV<?, ?>[] e = {//
                new XSDDoubleIV<BigdataLiteral>(1d),//
                new XSDDoubleIV<BigdataLiteral>(-1d),//
                new XSDDoubleIV<BigdataLiteral>(+0d),//
                new XSDDoubleIV<BigdataLiteral>(Double.MAX_VALUE),//
                new XSDDoubleIV<BigdataLiteral>(Double.MIN_VALUE),//
                new XSDDoubleIV<BigdataLiteral>(Double.MIN_NORMAL),//
                new XSDDoubleIV<BigdataLiteral>(Double.POSITIVE_INFINITY),//
                new XSDDoubleIV<BigdataLiteral>(Double.NEGATIVE_INFINITY),//
                new XSDDoubleIV<BigdataLiteral>(Double.NaN),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link UUIDLiteralIV}.
     */
    public void test_encodeDecode_UUID() {

        final IV<?, ?>[] e = new IV[100];
        
        for (int i = 0; i < e.length; i++) {
        
            e[i] = new UUIDLiteralIV<BigdataLiteral>(UUID.randomUUID());
            
        }

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /** Unit test for {@link XSDIntegerIV}. */
    public void test_encodeDecode_XSDInteger() {

        final IV<?, ?>[] e = {//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(-1L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(0L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MAX_VALUE)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MIN_VALUE)),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDIntegerIV} with positive and negative
     * {@link BigInteger}s having a common prefix with varying digits after the
     * prefix.
     */
    public void test_encodeDecode_XSDInteger_pos_and_neg_varying_digits() {

        final BigInteger p1 = new BigInteger("15");
        final BigInteger p2 = new BigInteger("151");
        final BigInteger m1 = new BigInteger("-15");
        final BigInteger m2 = new BigInteger("-151");

        final IV<?,?>[] e = new IV[] {
                new XSDIntegerIV<BigdataLiteral>(p1),//
                new XSDIntegerIV<BigdataLiteral>(p2),//
                new XSDIntegerIV<BigdataLiteral>(m1),//
                new XSDIntegerIV<BigdataLiteral>(m2),//
                };
        
        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Stress test for {@link XSDIntegerIV}.
     */
    public void test_encodeDecode_XSDInteger_stressTest() {

        final Random r = new Random();

        final List<IV<?,?>> a = new LinkedList<IV<?,?>>();
        
        for (int i = 0; i < 100; i++) {
            
            final BigInteger t1 = BigInteger.valueOf(r.nextLong());
            
            final BigInteger v2 = BigInteger.valueOf(Math.abs(r.nextLong()));
            
            final BigInteger v4 = BigInteger.valueOf(r.nextLong());
            
            // x LT t1
            final BigInteger t2 = t1.subtract(v2);
            final BigInteger t4 = t1.subtract(BigInteger.valueOf(5));
            final BigInteger t5 = t1.subtract(BigInteger.valueOf(9));

            // t1 LT x
            final BigInteger t3 = t1.add(v2);
            final BigInteger t6 = t1.add(BigInteger.valueOf(5));
            final BigInteger t7 = t1.add(BigInteger.valueOf(9));

            a.add(new XSDIntegerIV<BigdataLiteral>(t1));
            a.add(new XSDIntegerIV<BigdataLiteral>(v2));
            a.add(new XSDIntegerIV<BigdataLiteral>(v4));
            a.add(new XSDIntegerIV<BigdataLiteral>(t2));
            a.add(new XSDIntegerIV<BigdataLiteral>(t4));
            a.add(new XSDIntegerIV<BigdataLiteral>(t5));
            a.add(new XSDIntegerIV<BigdataLiteral>(t3));
            a.add(new XSDIntegerIV<BigdataLiteral>(t6));
            a.add(new XSDIntegerIV<BigdataLiteral>(t7));
            
        }
        
        final IV<?, ?>[] e = a.toArray(new IV[0]);

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test of {@link XSDDecimalIV} with positive and negative
     * {@link BigDecimal}s having varying digits after the decimals.
     */
    public void test_encodeDecode_XSDDecimal_pos_and_neg_varying_digits() {

        final BigDecimal p1 = new BigDecimal("1.5");
        final BigDecimal p2 = new BigDecimal("1.51");
        final BigDecimal m1 = new BigDecimal("-1.5");
        final BigDecimal m2 = new BigDecimal("-1.51");

        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(p2),//
                new XSDDecimalIV<BigdataLiteral>(m1),//
                new XSDDecimalIV<BigdataLiteral>(m2),//
                };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:decimal.
     */
    public void test_encodeDecode_XSDDecimal() {

        final BigDecimal z1 = new BigDecimal("0.0");
        final BigDecimal negz1 = new BigDecimal("-0.0");
        final BigDecimal z2 = new BigDecimal("0.00");
        final BigDecimal p1 = new BigDecimal("0.01");
        final BigDecimal negp1 = new BigDecimal("-0.01");
        final BigDecimal z3 = new BigDecimal("0000.00");
        final BigDecimal m1 = new BigDecimal("1.5");
        final BigDecimal m2 = new BigDecimal("-1.51");
        final BigDecimal m5 = new BigDecimal("5");
        final BigDecimal m53 = new BigDecimal("5.000");
        final BigDecimal m500 = new BigDecimal("00500");
        final BigDecimal m5003 = new BigDecimal("500.000");
        final BigDecimal v1 = new BigDecimal("383.00000000000001");
        final BigDecimal v2 = new BigDecimal("383.00000000000002");

        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(z1),//
                new XSDDecimalIV<BigdataLiteral>(negz1),//
                new XSDDecimalIV<BigdataLiteral>(z2),//
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(negp1),//
                new XSDDecimalIV<BigdataLiteral>(z3),//
                new XSDDecimalIV<BigdataLiteral>(m1),//
                new XSDDecimalIV<BigdataLiteral>(m2),//
                new XSDDecimalIV<BigdataLiteral>(m5),//
                new XSDDecimalIV<BigdataLiteral>(m53),//
                new XSDDecimalIV<BigdataLiteral>(m500),//
                new XSDDecimalIV<BigdataLiteral>(m5003),//
                new XSDDecimalIV<BigdataLiteral>(v1),//
                new XSDDecimalIV<BigdataLiteral>(v2),//
                };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDDecimalIV} with positive and negative values.
     */
    public void test_encodeDecode_XSDDecimal_pos_and_neg() {

        final BigDecimal p1 = new BigDecimal("12000000000000000000000000");
        final BigDecimal p2 = new BigDecimal("12000000000000000000000001");
        final BigDecimal p3 = new BigDecimal("1.201E25");
        final BigDecimal p4 = new BigDecimal("12020000000000000000000000");
        final BigDecimal p5 = new BigDecimal("1.201E260");
        final BigDecimal n1 = new BigDecimal("-12000000000000000000000000");
        final BigDecimal n2 = new BigDecimal("-12000000000000000000000001");
        final BigDecimal n3 = new BigDecimal("-1.2E260");
        final BigDecimal n4 = new BigDecimal("-1.201E260");
        
        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(p2),//
                new XSDDecimalIV<BigdataLiteral>(p3),//
                new XSDDecimalIV<BigdataLiteral>(p4),//
                new XSDDecimalIV<BigdataLiteral>(p5),//
                new XSDDecimalIV<BigdataLiteral>(n1),//
                new XSDDecimalIV<BigdataLiteral>(n2),//
                new XSDDecimalIV<BigdataLiteral>(n3),//
                new XSDDecimalIV<BigdataLiteral>(n4),//
                };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }
    
    /**
     * Unit test for {@link XSDDecimalIV}.
     */
    public void test_encodeDecode_XSDDecimal_2() {

        final IV<?,?>[] e = new IV[] {
            
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0)),//
            
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-123450)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-99)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-9)),//
            
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(8.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(255.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(256.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(512.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1028.001)),//

            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-8.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-255.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-256.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-512.0001)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1028.001)),//

            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MIN_VALUE)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MAX_VALUE)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MIN_VALUE - 1)),//
            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MAX_VALUE + 1)),//
            };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }
    
    /**
     * Unit test for {@link XSDDecimalIV}.
     */
    public void test_encodeDecode_XSDDecimal_3() {

      final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(2.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-2.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(10.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(11.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(258.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(3.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.02)),//
      };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Stress test for {@link XSDDecimalIV}.
     */
    public void test_encodeDecode_XSDDecimal_stressTest() {

        final Random r = new Random();

        final List<IV<?,?>> a = new LinkedList<IV<?,?>>();
        
        for (int i = 0; i < 100; i++) {
            
            final BigDecimal t1 = BigDecimal.valueOf(r.nextDouble());
            
            final BigDecimal v2 = BigDecimal.valueOf(Math.abs(r.nextDouble()));
            
            final BigDecimal v4 = BigDecimal.valueOf(r.nextDouble());
            
            // x LT t1
            final BigDecimal t2 = t1.subtract(v2);
            final BigDecimal t4 = t1.subtract(BigDecimal.valueOf(5));
            final BigDecimal t5 = t1.subtract(BigDecimal.valueOf(9));

            // t1 LT x
            final BigDecimal t3 = t1.add(v2);
            final BigDecimal t6 = t1.add(BigDecimal.valueOf(5));
            final BigDecimal t7 = t1.add(BigDecimal.valueOf(9));

            a.add(new XSDDecimalIV<BigdataLiteral>(t1));
            a.add(new XSDDecimalIV<BigdataLiteral>(v2));
            a.add(new XSDDecimalIV<BigdataLiteral>(v4));
            a.add(new XSDDecimalIV<BigdataLiteral>(t2));
            a.add(new XSDDecimalIV<BigdataLiteral>(t4));
            a.add(new XSDDecimalIV<BigdataLiteral>(t5));
            a.add(new XSDDecimalIV<BigdataLiteral>(t3));
            a.add(new XSDDecimalIV<BigdataLiteral>(t6));
            a.add(new XSDDecimalIV<BigdataLiteral>(t7));
            
        }
        
        final IV<?, ?>[] e = a.toArray(new IV[0]);

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link UUIDBNodeIV}, which provides support for inlining a
     * told blank node whose <code>ID</code> can be parsed as a {@link UUID}.
     */
    public void test_encodeDecode_BNode_UUID_ID() {

        final IV<?, ?>[] e = new IV[100];

        for (int i = 0; i < e.length; i++) {

            e[i] = new UUIDBNodeIV<BigdataBNode>(UUID.randomUUID());

        }

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link NumericBNodeIV}, which provides support for inlining
     * a told blank node whose <code>ID</code> can be parsed as an
     * {@link Integer}.
     */
    public void test_encodeDecode_BNode_INT_ID() {
        
        final IV<?, ?>[] e = {//
                new NumericBNodeIV<BigdataBNode>(-1),//
                new NumericBNodeIV<BigdataBNode>(0),//
                new NumericBNodeIV<BigdataBNode>(1),//
                new NumericBNodeIV<BigdataBNode>(-52),//
                new NumericBNodeIV<BigdataBNode>(52),//
                new NumericBNodeIV<BigdataBNode>(Integer.MAX_VALUE),//
                new NumericBNodeIV<BigdataBNode>(Integer.MIN_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for the {@link EpochExtension}.
     */
    public void test_encodeDecodeEpoch() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final EpochExtension<BigdataValue> ext = 
            new EpochExtension<BigdataValue>(new IDatatypeURIResolver() {
            public BigdataURI resolve(final URI uri) {
            	final BigdataURI buri = vf.createURI(uri.stringValue());
                buri.setIV(newTermId(VTE.URI));
                return buri;
            }
        });

        final Random r = new Random();
        
        final IV<?, ?>[] e = new IV[100];

        for (int i = 0; i < e.length; i++) {

            final long v = r.nextLong();

            final String s = Long.toString(v);
            
            final Literal lit = new LiteralImpl(s, EpochExtension.EPOCH);
            
            final IV<?,?> iv = ext.createIV(lit);

            if (iv == null)
                fail("Did not create IV: lit=" + lit);
            
            e[i] = iv;
            
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for the {@link ColorsEnumExtension}.
     */
    public void test_encodeDecodeColor() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final ColorsEnumExtension<BigdataValue> ext = 
            new ColorsEnumExtension<BigdataValue>(new IDatatypeURIResolver() {
            public BigdataURI resolve(URI uri) {
                final BigdataURI buri = vf.createURI(uri.stringValue());
                buri.setIV(newTermId(VTE.URI));
                return buri;
            }
        });
        
        final List<IV<?, ?>> a = new LinkedList<IV<?, ?>>();

        for (Color c : Color.values()) {
        
            a.add(ext.createIV(new LiteralImpl(c.name(),
                    ColorsEnumExtension.COLOR)));
            
        }

        final IV<?, ?>[] e = a.toArray(new IV[0]);

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for round-trip of xsd:dateTime values.
     */
    public void test_encodeDecodeDateTime() throws Exception {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final DatatypeFactory df = DatatypeFactory.newInstance();

        final DateTimeExtension<BigdataValue> ext = 
            new DateTimeExtension<BigdataValue>(new IDatatypeURIResolver() {
	            public BigdataURI resolve(URI uri) {
	                final BigdataURI buri = vf.createURI(uri.stringValue());
	                buri.setIV(newTermId(VTE.URI));
	                return buri;
	            }
	        },
	        TimeZone.getDefault()
            );
        
        final BigdataLiteral[] dt = {
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("2001-10-26T21:32:52")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("2001-10-26T21:32:52+02:00")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("2001-10-26T19:32:52Z")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("2001-10-26T19:32:52+00:00")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("-2001-10-26T21:32:52")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("2001-10-26T21:32:52.12679")),
    		vf.createLiteral(
        		df.newXMLGregorianCalendar("1901-10-26T21:32:52")),
        		};
        
        final IV<?, ?>[] e = new IV[dt.length];
        
        for (int i = 0; i < dt.length; i++) {

            e[i] = ext.createIV(dt[i]);
            
        }
        
        final IV<?, ?>[] a = doEncodeDecodeTest(e);

        if (log.isInfoEnabled()) {
        	for (int i = 0; i < e.length; i++) {
	        	log.info("original: "+dt[i]);
	        	log.info("asValue : "+ext.asValue((ExtensionIV) e[i], vf));
	        	log.info("decoded : "+ext.asValue((ExtensionIV) a[i], vf));
	        	log.info("");
	        }
//        	log.info(svf.createLiteral(
//                df.newXMLGregorianCalendar("2001-10-26T21:32:52.12679")));
        }
        
        doComparatorTest(e);
        
    }

    /**
     * Unit test verifies that the inline xsd:dateTime representation preserves
     * the milliseconds units. However, precision beyond milliseconds is NOT
     * preserved by the inline representation, which is based on milliseconds
     * since the epoch.
     * 
     * @throws DatatypeConfigurationException
     */
    public void test_dateTime_preservesMillis()
            throws DatatypeConfigurationException {

        final BigdataValueFactory vf = BigdataValueFactoryImpl
                .getInstance("test");

        final DatatypeFactory df = DatatypeFactory.newInstance();

        final DateTimeExtension<BigdataValue> ext = new DateTimeExtension<BigdataValue>(
                new IDatatypeURIResolver() {
                    public BigdataURI resolve(URI uri) {
                        final BigdataURI buri = vf.createURI(uri.stringValue());
                        buri.setIV(newTermId(VTE.URI));
                        return buri;
                    }
                }, TimeZone.getTimeZone("GMT"));

        /*
         * The string representation of the dateTime w/ milliseconds+ precision.
         * This is assumed to be a time in the time zone specified to the date
         * time extension.
         */
        final String givenStr = "2001-10-26T21:32:52.12679";

        /*
         * The string representation w/ only milliseconds precision. This will
         * be a time in the time zone given to the date time extension. The
         * canonical form of a GMT time zone is "Z", indicating "Zulu", which is
         * why that is part of the expected representation here.
         */
        final String expectedStr = "2001-10-26T21:32:52.126Z";

        /*
         * A bigdata literal w/o inlining from the *givenStr*. This
         * representation has greater milliseconds+ precision.
         */
        final BigdataLiteral lit = vf.createLiteral(df
                .newXMLGregorianCalendar(givenStr));
        
        // Verify the representation is exact.
        assertEquals(givenStr, lit.stringValue());

        /*
         * The IV representation of the dateTime. This will convert the date
         * time into the time zone given to the extension and will also truncate
         * the precision to no more than milliseconds.
         */
        final ExtensionIV<?> iv = ext.createIV(lit);

        // Convert the IV back into a bigdata literal.
        final BigdataLiteral lit2 = (BigdataLiteral) ext.asValue(iv, vf);

        // Verify that millisecond precision was retained.
        assertEquals(expectedStr, lit2.stringValue());

    }
    
    /**
     * Unit test for {@link SidIV}.
     */
    public void test_encodeDecode_sids() {
        
        final TermId<?> s1 = newTermId(VTE.URI);
        final TermId<?> s2 = newTermId(VTE.URI);
        final TermId<?> p1 = newTermId(VTE.URI);
        final TermId<?> p2 = newTermId(VTE.URI);
        final TermId<?> o1 = newTermId(VTE.URI);
        final TermId<?> o2 = newTermId(VTE.BNODE);
        final TermId<?> o3 = newTermId(VTE.LITERAL);

        final SPO spo1 = new SPO(s1, p1, o1, StatementEnum.Explicit);
        final SPO spo2 = new SPO(s1, p1, o2, StatementEnum.Explicit);
        final SPO spo3 = new SPO(s1, p1, o3, StatementEnum.Explicit);
        final SPO spo4 = new SPO(s1, p2, o1, StatementEnum.Explicit);
        final SPO spo5 = new SPO(s1, p2, o2, StatementEnum.Explicit);
        final SPO spo6 = new SPO(s1, p2, o3, StatementEnum.Explicit);
        final SPO spo7 = new SPO(s2, p1, o1, StatementEnum.Explicit);
        final SPO spo8 = new SPO(s2, p1, o2, StatementEnum.Explicit);
        final SPO spo9 = new SPO(s2, p1, o3, StatementEnum.Explicit);
        final SPO spo10 = new SPO(s2, p2, o1, StatementEnum.Explicit);
        final SPO spo11 = new SPO(s2, p2, o2, StatementEnum.Explicit);
        final SPO spo12 = new SPO(s2, p2, o3, StatementEnum.Explicit);
        spo1.setStatementIdentifier(true);
        spo2.setStatementIdentifier(true);
        spo3.setStatementIdentifier(true);
        spo6.setStatementIdentifier(true);
        final SPO spo13 = new SPO(spo1.getStatementIdentifier(), p1, o1,
                StatementEnum.Explicit);
        final SPO spo14 = new SPO(spo2.getStatementIdentifier(), p2, o2,
                StatementEnum.Explicit);
        final SPO spo15 = new SPO(s1, p1, spo3.getStatementIdentifier(),
                StatementEnum.Explicit);
        spo15.setStatementIdentifier(true);
        final SPO spo16 = new SPO(s1, p1, spo6.getStatementIdentifier(),
                StatementEnum.Explicit);
        final SPO spo17 = new SPO(spo1.getStatementIdentifier(), p1, spo15
                .getStatementIdentifier(), StatementEnum.Explicit);

        final IV<?, ?>[] e = {//
                new SidIV<BigdataBNode>(spo1),//
                new SidIV<BigdataBNode>(spo2),//
                new SidIV<BigdataBNode>(spo3),//
                new SidIV<BigdataBNode>(spo4),//
                new SidIV<BigdataBNode>(spo5),//
                new SidIV<BigdataBNode>(spo6),//
                new SidIV<BigdataBNode>(spo7),//
                new SidIV<BigdataBNode>(spo8),//
                new SidIV<BigdataBNode>(spo9),//
                new SidIV<BigdataBNode>(spo10),//
                new SidIV<BigdataBNode>(spo11),//
                new SidIV<BigdataBNode>(spo12),//
                new SidIV<BigdataBNode>(spo13),//
                new SidIV<BigdataBNode>(spo14),//
                new SidIV<BigdataBNode>(spo15),//
                new SidIV<BigdataBNode>(spo16),//
                new SidIV<BigdataBNode>(spo17),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for the {@link XSDStringExtension} support for inlining
     * <code>xsd:string</code>. This approach is more efficient since the
     * datatypeURI is implicit in the {@link IExtension} handler than being
     * explicitly represented in the inline data.
     */
    public void test_encodeDecode_extension_xsdString() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final int maxInlineStringLength = 128;
        
        final XSDStringExtension<BigdataValue> ext = 
            new XSDStringExtension<BigdataValue>(
                new IDatatypeURIResolver() {
                    public BigdataURI resolve(final URI uri) {
                        final BigdataURI buri = vf.createURI(uri.stringValue());
                        buri.setIV(newTermId(VTE.URI));
                        return buri;
                    }
                }, maxInlineStringLength);
        
        final IV<?, ?>[] e = {//
                ext.createIV(new LiteralImpl("", XSD.STRING)), //
                ext.createIV(new LiteralImpl(" ", XSD.STRING)), //
                ext.createIV(new LiteralImpl("  ", XSD.STRING)), //
                ext.createIV(new LiteralImpl("1", XSD.STRING)), //
                ext.createIV(new LiteralImpl("12", XSD.STRING)), //
                ext.createIV(new LiteralImpl("123", XSD.STRING)), //
                ext.createIV(new LiteralImpl("234", XSD.STRING)), //
                ext.createIV(new LiteralImpl("34", XSD.STRING)), //
                ext.createIV(new LiteralImpl("4", XSD.STRING)), //
                ext.createIV(new LiteralImpl("a", XSD.STRING)), //
                ext.createIV(new LiteralImpl("ab", XSD.STRING)), //
                ext.createIV(new LiteralImpl("abc", XSD.STRING)), //
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);
        
    }

    /**
     * Unit test for inlining blank nodes having a Unicode <code>ID</code>.
     */
    public void test_encodeDecode_Inline_BNode_UnicodeID() {

        final IV<?, ?>[] e = {//
                new UnicodeBNodeIV<BigdataBNode>("FOO"),//
                new UnicodeBNodeIV<BigdataBNode>("_bar"),//
                new UnicodeBNodeIV<BigdataBNode>("bar"),//
                new UnicodeBNodeIV<BigdataBNode>("baz"),//
                new UnicodeBNodeIV<BigdataBNode>("12"),//
                new UnicodeBNodeIV<BigdataBNode>("1298"),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests the inlining of plain literals.
     */
    public void test_encodeDecode_Inline_Literal_plainLiteral() {
        
        final IV<?, ?>[] e = {//
                new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("123", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("23", null/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("3", null/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("", null/* language */,
						null/* datatype */),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests inlining of language code literals.
     */
    public void test_encodeDecode_Inline_Literal_languageCodeLiteral() {
        
        final IV<?, ?>[] e = {//
                new InlineLiteralIV<BigdataLiteral>("foo", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("goo", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("foo", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("goo", "de"/* language */,
                        null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("baz", "de"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("", "en"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("", "de"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("1", "en"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("1", "de"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("12", "en"/* language */,
						null/* datatype */),//
				new InlineLiteralIV<BigdataLiteral>("12", "de"/* language */,
						null/* datatype */),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests inlining of datatype literals which DO NOT correspond to
     * registered extension types as the datatypeIV plus the inline Unicode
     * value of the label.
     */
    public void test_encodeDecode_Inline_Literal_datatypeLiteral() {
        
        final URI dt1 = new URIImpl("http://www.bigdata.com/mock-datatype-1");
        final URI dt2 = new URIImpl("http://www.bigdata.com/mock-datatype-2");
        
		final IV<?, ?>[] e = {//
				new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
						dt1),//
				new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
						dt1),//
				new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
						dt1),//
				new InlineLiteralIV<BigdataLiteral>("goo", null/* language */,
						dt1),//
				new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
						dt2),//
				new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
						dt2),//
				new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
						dt2),//
				new InlineLiteralIV<BigdataLiteral>("goo", null/* language */,
						dt2),//
				new InlineLiteralIV<BigdataLiteral>("", null/* language */, dt2),//
				new InlineLiteralIV<BigdataLiteral>("", null/* language */, dt2),//
				new InlineLiteralIV<BigdataLiteral>("1", null/* language */, dt2),//
				new InlineLiteralIV<BigdataLiteral>("1", null/* language */, dt2),//
				new InlineLiteralIV<BigdataLiteral>("12", null/* language */, dt2),//
				new InlineLiteralIV<BigdataLiteral>("12", null/* language */, dt2),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * A unit test for {@link InlineLiteralIV} in which we mix plain literals,
     * language code literals, and datatype literals. This verifies that they
     * encode and decode correctly but also that the unsigned byte[] ordering of
     * the encoded keys is preserved across the different types of literals.
     */
    public void test_encodeDecode_Inline_Literals_All_Types() {
        
        final URI dt1 = new URIImpl("http://www.bigdata.com/mock-datatype-1");
        final URI dt2 = new URIImpl("http://www.bigdata.com/mock-datatype-2");
        
        final IV<?, ?>[] e = {//
                /*
                 * Plain literals.
                 */
                new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("123", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("23", null/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("3", null/* language */,
                        null/* datatype */),//
                /*
                 * Language code literals.
                 */
                new InlineLiteralIV<BigdataLiteral>("foo", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("goo", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("foo", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("goo", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", "de"/* language */,
                        null/* datatype */),//
                /*
                 * Datatype literals.
                 */
                new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
                        dt1),//
                new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
                        dt1),//
                new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
                        dt1),//
                new InlineLiteralIV<BigdataLiteral>("goo", null/* language */,
                        dt1),//
                new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
                        dt2),//
                new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
                        dt2),//
                new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
                        dt2),//
                new InlineLiteralIV<BigdataLiteral>("goo", null/* language */,
                        dt2),//
        };
        
        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }
    
    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests for possible conflicting interpretations of an xsd:string
     * value. The interpretation as a fully inline literal should be distinct
     * from other possible interpretations so this is testing for unexpected
     * errors.
     */
    public void test_encodeDecode_Inline_Literal_XSDString_DeconflictionTest() {
    
        final IV<?, ?>[] e = {//
                new InlineLiteralIV<BigdataLiteral>("foo", null/* language */,
                        XSD.STRING/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("bar", null/* language */,
                        XSD.STRING/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("baz", null/* language */,
                        XSD.STRING/* datatype */),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for inlining an entire URI using {@link InlineURIIV}. The URI
     * is inlined as a Unicode component using {@link DTE#XSDString}. The
     * extension bit is NOT set since we are not factoring out the namespace
     * component of the URI.
     */
    public void test_encodeDecode_Inline_URI() {
        
        final IV<?, ?>[] e = {//
                new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com")),//
                new InlineURIIV<BigdataURI>(RDF.TYPE),//
                new InlineURIIV<BigdataURI>(RDF.SUBJECT),//
                new InlineURIIV<BigdataURI>(RDF.BAG),//
                new InlineURIIV<BigdataURI>(RDF.OBJECT),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);
        
    }

	/**
	 * Unit test for a fully inlined representation of a URI based on a
	 * <code>byte</code> code. The flags byte looks like:
	 * <code>VTE=URI, inline=true, extension=false,
	 * DTE=XSDByte</code>. It is followed by a <code>unsigned byte</code> value
	 * which is the index of the URI in the {@link Vocabulary} class for the
	 * triple store.
	 */
    public void test_encodeDecode_URIByteIV() {

        final IV<?, ?>[] e = {//
				new URIByteIV<BigdataURI>((byte) Byte.MIN_VALUE),//
				new URIByteIV<BigdataURI>((byte) -1),//
				new URIByteIV<BigdataURI>((byte) 0),//
				new URIByteIV<BigdataURI>((byte) 1),//
				new URIByteIV<BigdataURI>((byte) Byte.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

	/**
	 * Unit test for a fully inlined representation of a URI based on a
	 * <code>short</code> code. The flags byte looks like:
	 * <code>VTE=URI, inline=true, extension=false,
	 * DTE=XSDShort</code>. It is followed by an <code>unsigned short</code>
	 * value which is the index of the URI in the {@link Vocabulary} class for
	 * the triple store.
	 */
    public void test_encodeDecode_URIShortIV() {

        final IV<?, ?>[] e = {//
				new URIShortIV<BigdataURI>((short) Short.MIN_VALUE),//
				new URIShortIV<BigdataURI>((short) -1),//
				new URIShortIV<BigdataURI>((short) 0),//
				new URIShortIV<BigdataURI>((short) 1),//
				new URIShortIV<BigdataURI>((short) Short.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Test for a URI broken down into namespace and local name components. The
     * namespace component is coded by setting the extension bit and placing the
     * IV of the namespace into the extension IV field. The local name is
     * inlined as a Unicode component using {@link DTE#XSDString}.
     */
    public void test_encodeDecode_NonInline_URI_with_NamespaceIV() {

        final TermId namespaceIV = newTermId(VTE.URI);
        
        final IV<?, ?>[] e = {//
                newTermId(VTE.URI),//
                newTermId(VTE.URI),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), namespaceIV),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);
        
    }

    /**
     * Test for a literal broken down into datatype IV and an inline label. The
     * datatype IV is coded by setting the extension bit and placing the IV of
     * the namespace into the extension IV field. The local name is inlined as a
     * Unicode component using {@link DTE#XSDString}.
     */
    public void test_encodeDecode_NonInline_Literal_with_DatatypeIV() {

        final TermId<?> datatypeIV = newTermId(VTE.URI);
        final TermId<?> datatypeIV2 = newTermId(VTE.URI);
        
        final IV<?, ?>[] e = {//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>(""), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>(" "), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("1"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("12"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("baz"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("baz"), datatypeIV2),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

	/**
	 * Unit test for a fully inline representation of a URI based on a
	 * namespaceIV represented by a {@link URIShortIV} and a Unicode localName.
	 */
    public void test_encodeDecode_URINamespaceIV() {

        final IV<?, ?>[] e = {//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("bar"),// localName
                        new URIShortIV<BigdataURI>((short) 1) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("baz"),// localName
                        new URIShortIV<BigdataURI>((short) 1) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("bar"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("baz"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
        };

        doEncodeDecodeTest(e);
     
        doComparatorTest(e);

    }

	/**
	 * Unit test for a fully inline representation of a datatype Literal based
	 * on a datatypeIV represented by a {@link URIShortIV} and a Unicode
	 * localName.
	 */
    public void test_encodeDecode_LiteralNamespaceIV() {

        final IV datatypeIV = new URIShortIV<BigdataURI>((short) 1);
        final IV datatypeIV2 = new URIShortIV<BigdataURI>((short) 2);

        final IV<?, ?>[] e = {//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("baz"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("baz"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("123"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("123"), datatypeIV2),//
        };

        doEncodeDecodeTest(e);
     
        doComparatorTest(e);

    }

    /**
     * Verify the comparator for some set of {@link IV}s.
     * 
     * @param e
     */
    static void doComparatorTest(final IV<?,?>[] e) {

        final TermsIndexHelper h = new TermsIndexHelper();
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();
        
        final byte[][] keys = new byte[e.length][];
        for (int i = 0; i < e.length; i++) {
        
            final IV<?, ?> iv = e[i];
            
            // Encode as key.
            final byte[] key = IVUtility.encode(keyBuilder.reset(), iv).getKey();
            
            // Decode the key.
            final IV<?, ?> actualIV = IVUtility.decode(key);

            // Must compare as equal() for the rest of this logic to work.
            assertEquals(iv, actualIV);
            
            // Save the key.
            keys[i] = key;
            
        }

        // Clone the caller's array to avoid side effects.
        final IV<?, ?>[] a = e.clone();

        // Place into order according to the IV's Comparable implementation.
        Arrays.sort(a);

        // Place into unsigned byte[] order.
        Arrays.sort(keys, UnsignedByteArrayComparator.INSTANCE);
        
        for (int i = 0; i < e.length; i++) {

            // unsigned byte[] ordering.
            final IV<?,?> expectedIV = IVUtility.decode(keys[i]);
            
            // IV's Comparable ordering.
            final IV<?,?> actualIV = a[i];
            
            if (!expectedIV.equals(actualIV)) {

                /*
                 * The IV's Comparable does not agree with the required unsigned
                 * byte[] ordering semantics.
                 */
                
                fail("Order differs at index=" + i + ": expectedIV="
                        + expectedIV + ", actualIV=" + actualIV);
                
            }
            
        }

    }
    
}
