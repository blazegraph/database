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
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import junit.framework.TestCase2;

import org.deri.iris.basics.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;

/**
 * Unit tests for encoding and decoding compound keys (such as are used by the
 * statement indices) in which some of the key components are inline values
 * having variable component lengths while others are term identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2756 2010-05-03 22:26:18Z thompsonbry$
 */
public class TestEncodeDecodeKeys extends TestCase2 {

    public TestEncodeDecodeKeys() {
        super();
    }
    
    public TestEncodeDecodeKeys(String name) {
        super(name);
    }

    /**
     * Unit tests for {@link TermId}
     */
    public void test_TermId() {

        final Random r = new Random();
        
        for(VTE vte : VTE.values()) {
//        InternalValueTypeEnum vte = InternalValueTypeEnum.BNODE;
//        {

//            for(DTE dte : DTE.values()) {

                // 64 bit random term identifier.
                final long termId = r.nextLong();

                final TermId<?> v = new TermId<BigdataValue>(vte, termId);

                assertTrue(v.isTermId());

                assertFalse(v.isInline());
                
                assertEquals(termId, v.getTermId());

                try {
                    v.getInlineValue();
                    fail("Expecting "+UnsupportedOperationException.class);
                } catch(UnsupportedOperationException ex) {
                    // ignored.
                }
                
                assertEquals("flags="+v.flags(), vte, v.getVTE());

//                assertEquals(dte, v.getInternalDataTypeEnum());

                switch(vte) {
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

//            }
            
        }
        
    }

    public void test_InlineValue() {

//        final Random r = new Random();

        for (VTE vte : VTE.values()) {

            if (vte.equals(VTE.URI)) {
                // We do not inline URIs.
                continue;
            }

            for (DTE dte : DTE.values()) {

//                // 64 bit random term identifier.
//                final long termId = r.nextLong();

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

                    public long getTermId() {
                        throw new UnsupportedOperationException();
                    }

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

                assertEquals("flags=" + v.flags(), vte, v
                        .getVTE());

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
    public IV[] decodeStatementKey(final byte[] key, final int arity) {

        return IVUtility.decode(key, arity);
        
    }

    /**
     * Encodes an array of {@link IV}s and then decodes them and
     * verifies that the decoded values are equal-to the original values.
     * 
     * @param e
     *            The array of the expected values.
     */
    protected IV<?, ?>[] doEncodeDecodeTest(final IV<?, ?>[] e) {

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
        {
            final IV<?, ?>[] a = decodeStatementKey(key, e.length);

            for (int i = 0; i < e.length; i++) {

                if (!e[i].equals(a[i])) {
                 
                    fail("index=" + Integer.toString(i) + " : expected=" + e[i]
                            + ", actual=" + a[i]);
                    
                }

            }

            return a;
            
        }

    }

    /**
     * Unit test for encoding and decoding a statement formed from
     * {@link TermId}s.
     */
    public void test_SPO_encodeDecode_allTermIds() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new TermId<BigdataURI>(VTE.URI, 3L),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Unit test where the RDF Object position is an xsd:boolean.
     */
    public void test_SPO_encodeDecode_XSDBoolean() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDBooleanIV<BigdataLiteral>(true),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDBooleanIV<BigdataLiteral>(false);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:byte.
     */
    public void test_SPO_encodeDecode_XSDByte() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDByteIV<BigdataLiteral>((byte)1),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDByteIV<BigdataLiteral>((byte) -1);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteIV<BigdataLiteral>((byte) 0);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteIV<BigdataLiteral>(Byte.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDByteIV<BigdataLiteral>(Byte.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:short.
     */
    public void test_SPO_encodeDecode_XSDShort() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDShortIV<BigdataLiteral>((short)1),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDShortIV<BigdataLiteral>((short) -1);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortIV<BigdataLiteral>((short) 0);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortIV<BigdataLiteral>(Short.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDShortIV<BigdataLiteral>(Short.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:int.
     */
    public void test_SPO_encodeDecode_XSDInt() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDIntIV<BigdataLiteral>(1),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDIntIV<BigdataLiteral>(-1);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntIV<BigdataLiteral>(0);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntIV<BigdataLiteral>(Integer.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDIntIV<BigdataLiteral>(Integer.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:long.
     */
    public void test_SPO_encodeDecode_XSDLong() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDLongIV<BigdataLiteral>(1L),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDLongIV<BigdataLiteral>(-1L);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongIV<BigdataLiteral>(0L);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongIV<BigdataLiteral>(Long.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDLongIV<BigdataLiteral>(Long.MIN_VALUE);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:float.
     */
    public void test_SPO_encodeDecode_XSDFloat() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDFloatIV<BigdataLiteral>(1f),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(-1f);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(+0f);

        doEncodeDecodeTest(e);

        // Note: -0f and +0f are converted to the same point in the value space. 
//        e[2] = new XSDFloatIV<BigdataLiteral>(-0f);
//
//        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.MIN_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.MIN_NORMAL);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.POSITIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.NEGATIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDFloatIV<BigdataLiteral>(Float.NaN);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is an xsd:double.
     */
    public void test_SPO_encodeDecode_XSDDouble() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDDoubleIV<BigdataLiteral>(1d),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(-1d);

        doEncodeDecodeTest(e);

        // Note: -0d and +0d are converted to the same point in the value space. 
//        e[2] = new XSDDoubleIV<BigdataLiteral>(-0d);
//
//        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(+0d);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.MAX_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.MIN_VALUE);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.MIN_NORMAL);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.POSITIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.NEGATIVE_INFINITY);

        doEncodeDecodeTest(e);

        e[2] = new XSDDoubleIV<BigdataLiteral>(Double.NaN);

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test where the RDF Object position is a {@link UUID}.
     */
    public void test_SPO_encodeDecode_UUID() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new UUIDLiteralIV<BigdataLiteral>(UUID.randomUUID()),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);

        for (int i = 0; i < 1000; i++) {

            e[2] = new UUIDLiteralIV<BigdataLiteral>(UUID.randomUUID());

            doEncodeDecodeTest(e);

        }

    }

    /**
     * Unit test where the RDF Object position is an xsd:integer.
     */
    public void test_SPO_encodeDecode_XSDInteger() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger
                        .valueOf(3L)),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Unit test where the RDF Object position is an xsd:decimal.
     */
    public void test_SPO_encodeDecode_XSDDecimal() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal
                        .valueOf(3.3d)),//
                new TermId<BigdataURI>(VTE.URI, 4L) //
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Unit test for {@link UUIDBNodeIV}, which provides support for inlining a
     * told blank node whose <code>ID</code> can be parsed as a {@link UUID}.
     */
    public void test_SPO_encodeDecode_BNode_UUID_ID() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new UUIDBNodeIV<BigdataBNode>(UUID.randomUUID()),//
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for {@link NumericBNodeIV}, which provides support for inlining
     * a told blank node whose <code>ID</code> can be parsed as an
     * {@link Integer}.
     */
    public void test_SPO_encodeDecode_BNode_INT_ID() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new NumericBNodeIV<BigdataBNode>(0),//
                new NumericBNodeIV<BigdataBNode>(52),//
                new NumericBNodeIV<BigdataBNode>(Integer.MAX_VALUE),//
                new NumericBNodeIV<BigdataBNode>(Integer.MIN_VALUE),//
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for the {@link EpochExtension}.
     */
    public void test_SPO_encodeDecodeEpoch() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final EpochExtension<BigdataValue> ext = 
            new EpochExtension<BigdataValue>(new IDatatypeURIResolver() {
            public BigdataURI resolve(URI uri) {
                BigdataURI buri = vf.createURI(uri.stringValue());
                buri.setIV(new TermId(VTE.URI, 1024));
                return buri;
            }
        });
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                ext.createIV(new LiteralImpl("1234", EpochExtension.EPOCH)),
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for the {@link ColorsEnumExtension}.
     */
    public void test_SPO_encodeDecodeColor() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final ColorsEnumExtension<BigdataValue> ext = 
            new ColorsEnumExtension<BigdataValue>(new IDatatypeURIResolver() {
            public BigdataURI resolve(URI uri) {
                BigdataURI buri = vf.createURI(uri.stringValue());
                buri.setIV(new TermId(VTE.URI, 1024));
                return buri;
            }
        });
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                ext.createIV(new LiteralImpl("Blue", ColorsEnumExtension.COLOR)),
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for round-trip of xsd:dateTime values.
     */
    public void test_SPO_encodeDecodeDateTime() throws Exception {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final DatatypeFactory df = DatatypeFactory.newInstance();

        final DateTimeExtension<BigdataValue> ext = 
            new DateTimeExtension<BigdataValue>(new IDatatypeURIResolver() {
	            public BigdataURI resolve(URI uri) {
	                final BigdataURI buri = vf.createURI(uri.stringValue());
	                buri.setIV(new TermId(VTE.URI, 1024));
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
        
        for (int i = 0; i < dt.length; i++)
            e[i] = ext.createIV(dt[i]);
        
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
                        buri.setIV(new TermId(VTE.URI, 1024));
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
    
    public void test_SPO_encodeDecodeSids() {
        
    	final SPO spo = new SPO(
    			new TermId(VTE.URI, 1L), 
    			new TermId(VTE.URI, 2L), 
    			new TermId(VTE.URI, 4L),
    			StatementEnum.Explicit);
    	
        final IV<?, ?>[] e = {//
                new SidIV(spo),//
                new TermId(VTE.URI, 8L),//
                new TermId(VTE.URI, 16L) //
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for the {@link XSDStringExtension} support for inlining
     * <code>xsd:string</code>. This approach is more efficient since the
     * datatypeURI is implicit in the {@link IExtension} handler than being
     * explicitly represented in the inline data.
     */
    public void test_SPO_encodeDecode_extension_xsdString() {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final int maxInlineStringLength = 128;
        
        final XSDStringExtension<BigdataValue> ext = 
            new XSDStringExtension<BigdataValue>(
                new IDatatypeURIResolver() {
                    public BigdataURI resolve(URI uri) {
                        final BigdataURI buri = vf.createURI(uri.stringValue());
                        buri.setIV(new TermId(VTE.URI, 1024));
                        return buri;
                    }
                }, maxInlineStringLength);
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                ext.createIV(new LiteralImpl("1234", XSD.STRING)),
        };

        doEncodeDecodeTest(e);
    }

    /**
     * Unit test for inlining blank nodes having a Unicode <code>ID</code>.
     */
    public void test_SPO_encodeDecode_Inline_BNode_UnicodeID() {

        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new UnicodeBNodeIV<BigdataBNode>("FOO"),//
                new UnicodeBNodeIV<BigdataBNode>("_bar"),//
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests the inlining of plain literals.
     */
    public void test_SPO_encodeDecode_Inline_Literal_plainLiteral() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new InlineLiteralIV<BigdataLiteral>("FOO", null/* language */,
                        null/* datatype */),//
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for {@link InlineLiteralIV}. That class provides inlining of
     * any kind of {@link Literal}. However, while that class is willing to
     * inline <code>xsd:string</code> it is more efficient to handle inlining
     * for <code>xsd:string</code> using the {@link XSDStringExtension}.
     * <p>
     * This tests inlining of language code literals.
     */
    public void test_SPO_encodeDecode_Inline_Literal_languageCodeLiteral() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new InlineLiteralIV<BigdataLiteral>("GOO", "en"/* language */,
                        null/* datatype */),//
        };

        doEncodeDecodeTest(e);

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
    public void test_SPO_encodeDecode_Inline_Literal_datatypeLiteral() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new InlineLiteralIV<BigdataLiteral>("BAR", null/* language */,
                        new URIImpl("http://www.bigdata.com")/* datatype */),//
        };

        doEncodeDecodeTest(e);

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
    public void test_SPO_encodeDecode_Inline_Literal_XSDString_DeconflictionTest() {
    
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new InlineLiteralIV<BigdataLiteral>("BAR", null/* language */,
                        XSD.STRING/* datatype */),//
        };

        doEncodeDecodeTest(e);

    }

    /**
     * Unit test for inlining an entire URI using {@link InlineURIIV}. The URI
     * is inlined as a Unicode component using {@link DTE#XSDString}. The
     * extension bit is NOT set since we are not factoring out the namespace
     * component of the URI.
     */
    public void test_SPO_encodeDecode_Inline_URI() {
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com")),//
                new InlineURIIV<BigdataURI>(RDF.TYPE),//
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Test for a URI broken down into namespace and local name components. The
     * namespace component is coded by setting the extension bit and placing the
     * IV of the namespace into the extension IV field. The local name is
     * inlined as a Unicode component using {@link DTE#XSDString}.
     */
    public void test_SPO_encodeDecode_NonInline_URI_with_NamespaceIV() {

        final TermId namespaceIV = new TermId<BigdataURI>(VTE.URI, 3L);
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), namespaceIV),//
        };

        doEncodeDecodeTest(e);
        
    }

    /**
     * Test for a literal broken down into datatype IV and an inline label. The
     * datatype IV is coded by setting the extension bit and placing the IV of
     * the namespace into the extension IV field. The local name is inlined as a
     * Unicode component using {@link DTE#XSDString}.
     */
    public void test_SPO_encodeDecode_NonInline_Literal_with_DatatypeIV() {

        final TermId datatypeIV = new TermId<BigdataURI>(VTE.URI, 3L);
        
        final IV<?, ?>[] e = {//
                new TermId<BigdataURI>(VTE.URI, 1L),//
                new TermId<BigdataURI>(VTE.URI, 2L),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), datatypeIV),//
        };

        doEncodeDecodeTest(e);

    }
    
}
