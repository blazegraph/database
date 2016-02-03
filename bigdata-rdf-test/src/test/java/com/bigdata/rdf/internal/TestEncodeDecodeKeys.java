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
 * Created on Apr 19, 2010
 */

package com.bigdata.rdf.internal;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;

import com.bigdata.rdf.internal.ColorsEnumExtension.Color;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.extensions.DerivedNumericsExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.service.geospatial.GeoSpatial;

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
public class TestEncodeDecodeKeys extends AbstractEncodeDecodeKeysTestCase {

    public TestEncodeDecodeKeys() {
        super();
    }
    
    public TestEncodeDecodeKeys(String name) {
        super(name);
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
                    
                    @Override
                    public int byteLength() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int hashCode() {
                        return 0;
                    }

                    @Override
                    public IV<?, ?> clone(boolean clearCache) {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public int _compareTo(IV o) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BigdataValue asValue(final LexiconRelation lex)
                            throws UnsupportedOperationException {
                        return null;
                    }

                    @Override
                    public Object getInlineValue()
                            throws UnsupportedOperationException {
                        return null;
                    }

                    @Override
                    public boolean isInline() {
                        return true;
                    }
                    
                    @Override
                    public boolean needsMaterialization() {
                        return false;
                    }

                    @Override
                    public String stringValue() {
                        throw new UnsupportedOperationException();
                    }

                };

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
     * Unit test for encoding and decoding a statement formed from
     * {@link BlobIV}s.
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
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDByte() {

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>((byte)Byte.MIN_VALUE),//
                new XSDNumericIV<BigdataLiteral>((byte)-1),//
                new XSDNumericIV<BigdataLiteral>((byte)0),//
                new XSDNumericIV<BigdataLiteral>((byte)1),//
                new XSDNumericIV<BigdataLiteral>((byte)Byte.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDShort() {

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>((short)-1),//
                new XSDNumericIV<BigdataLiteral>((short)0),//
                new XSDNumericIV<BigdataLiteral>((short)1),//
                new XSDNumericIV<BigdataLiteral>((short)Short.MIN_VALUE),//
                new XSDNumericIV<BigdataLiteral>((short)Short.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDInt() {

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>(1),//
                new XSDNumericIV<BigdataLiteral>(0),//
                new XSDNumericIV<BigdataLiteral>(-1),//
                new XSDNumericIV<BigdataLiteral>(Integer.MAX_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Integer.MIN_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDLong() {

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>(1L),//
                new XSDNumericIV<BigdataLiteral>(0L),//
                new XSDNumericIV<BigdataLiteral>(-1L),//
                new XSDNumericIV<BigdataLiteral>(Long.MIN_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Long.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDFloat() {

        /*
         * Note: -0f and +0f are converted to the same point in the value space.
         */
//        new XSDNumericIV<BigdataLiteral>(-0f);

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>(1f),//
                new XSDNumericIV<BigdataLiteral>(-1f),//
                new XSDNumericIV<BigdataLiteral>(+0f),//
                new XSDNumericIV<BigdataLiteral>(Float.MAX_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Float.MIN_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Float.MIN_NORMAL),//
                new XSDNumericIV<BigdataLiteral>(Float.POSITIVE_INFINITY),//
                new XSDNumericIV<BigdataLiteral>(Float.NEGATIVE_INFINITY),//
                new XSDNumericIV<BigdataLiteral>(Float.NaN),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDNumericIV}.
     */
    public void test_encodeDecode_XSDDouble() {

        /*
         * Note: -0d and +0d are converted to the same point in the value space.
         */
//      new XSDNumericIV<BigdataLiteral>(-0d);

        final IV<?, ?>[] e = {//
                new XSDNumericIV<BigdataLiteral>(1d),//
                new XSDNumericIV<BigdataLiteral>(-1d),//
                new XSDNumericIV<BigdataLiteral>(+0d),//
                new XSDNumericIV<BigdataLiteral>(Double.MAX_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Double.MIN_VALUE),//
                new XSDNumericIV<BigdataLiteral>(Double.MIN_NORMAL),//
                new XSDNumericIV<BigdataLiteral>(Double.POSITIVE_INFINITY),//
                new XSDNumericIV<BigdataLiteral>(Double.NEGATIVE_INFINITY),//
                new XSDNumericIV<BigdataLiteral>(Double.NaN),//
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
                log.info("original: " + dt[i]);
                log.info("asValue : " + ext.asValue((LiteralExtensionIV<?>) e[i], vf));
                log.info("decoded : " + ext.asValue((LiteralExtensionIV<?>) a[i], vf));
                log.info("");
            }
//          log.info(svf.createLiteral(
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
        final LiteralExtensionIV<?> iv = ext.createIV(lit);

        // Convert the IV back into a bigdata literal.
        final BigdataLiteral lit2 = (BigdataLiteral) ext.asValue(iv, vf);

        // Verify that millisecond precision was retained.
        assertEquals(expectedStr, lit2.stringValue());

    }
    
    /**
     * Unit test for round-trip of derived numeric values.
     */
    public void test_encodeDecodeDerivedNumerics() throws Exception {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final DatatypeFactory df = DatatypeFactory.newInstance();

        final DerivedNumericsExtension<BigdataValue> ext = 
            new DerivedNumericsExtension<BigdataValue>(new IDatatypeURIResolver() {
                public BigdataURI resolve(URI uri) {
                    final BigdataURI buri = vf.createURI(uri.stringValue());
                    buri.setIV(newTermId(VTE.URI));
                    return buri;
                }
            });
        
        final BigdataLiteral[] dt = {
            vf.createLiteral("1", XSD.POSITIVE_INTEGER),
            vf.createLiteral("-1", XSD.NEGATIVE_INTEGER),
            vf.createLiteral("-1", XSD.NON_POSITIVE_INTEGER),
            vf.createLiteral("1", XSD.NON_NEGATIVE_INTEGER),
            vf.createLiteral("0", XSD.NON_POSITIVE_INTEGER),
            vf.createLiteral("0", XSD.NON_NEGATIVE_INTEGER),
                };
        
        final IV<?, ?>[] e = new IV[dt.length];
        
        for (int i = 0; i < dt.length; i++) {

            e[i] = ext.createIV(dt[i]);
            
        }
        
        final IV<?, ?>[] a = doEncodeDecodeTest(e);

        if (log.isInfoEnabled()) {
            for (int i = 0; i < e.length; i++) {
                log.info("original: " + dt[i]);
                log.info("asValue : " + ext.asValue((LiteralExtensionIV<?>) e[i], vf));
                log.info("decoded : " + ext.asValue((LiteralExtensionIV<?>) a[i], vf));
                log.info("");
            }
//          log.info(svf.createLiteral(
//                df.newXMLGregorianCalendar("2001-10-26T21:32:52.12679")));
        }
        
        doComparatorTest(e);
        
    }

    /**
     * Unit test for round-trip of GeoSpatial literals
     */
    public void test_encodeDecodeGeoSpatialLiterals01() throws Exception {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final GeoSpatialLiteralExtension<BigdataValue> ext = 
            new GeoSpatialLiteralExtension<BigdataValue>(new IDatatypeURIResolver() {
                public BigdataURI resolve(URI uri) {
                    final BigdataURI buri = vf.createURI(uri.stringValue());
                    buri.setIV(newTermId(VTE.URI));
                    return buri;
                }
            });
        
        final BigdataLiteral[] dt = {
           vf.createLiteral("2#2#1", GeoSpatial.DATATYPE),
           vf.createLiteral("3#3#1", GeoSpatial.DATATYPE),
           vf.createLiteral("4#4#1", GeoSpatial.DATATYPE),
           vf.createLiteral("5#5#1", GeoSpatial.DATATYPE),
           vf.createLiteral("6#6#1", GeoSpatial.DATATYPE),
           vf.createLiteral("7#7#1", GeoSpatial.DATATYPE),
        };
        
        final IV<?, ?>[] e = new IV[dt.length];
        
        for (int i = 0; i < dt.length; i++) {

            e[i] = ext.createIV(dt[i]);
            
        }
        
        final IV<?, ?>[] a = doEncodeDecodeTest(e);

        if (log.isInfoEnabled()) {
            for (int i = 0; i < e.length; i++) {
                log.info("original: " + dt[i]);
                log.info("asValue : " + ext.asValue((LiteralExtensionIV<?>) e[i], vf));
                log.info("decoded : " + ext.asValue((LiteralExtensionIV<?>) a[i], vf));
                log.info("");
            }
//          log.info(svf.createLiteral(
//                df.newXMLGregorianCalendar("2001-10-26T21:32:52.12679")));
        }
        
        doComparatorTest(e);
        
    }
    
    /**
     * Unit test for round-trip of GeoSpatial literals
     */
    public void test_encodeDecodeGeoSpatialLiterals02() throws Exception {
        
        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
        
        final GeoSpatialLiteralExtension<BigdataValue> ext = 
            new GeoSpatialLiteralExtension<BigdataValue>(new IDatatypeURIResolver() {
                public BigdataURI resolve(URI uri) {
                    final BigdataURI buri = vf.createURI(uri.stringValue());
                    buri.setIV(newTermId(VTE.URI));
                    return buri;
                }
            });
        
        final BigdataLiteral[] dt = {
           vf.createLiteral("8#8#1", GeoSpatial.DATATYPE)
        };
        
        final IV<?, ?>[] e = new IV[dt.length];
        
        for (int i = 0; i < dt.length; i++) {

            e[i] = ext.createIV(dt[i]);
            
        }
        
        final IV<?, ?>[] a = doEncodeDecodeTest(e);

        if (log.isInfoEnabled()) {
            for (int i = 0; i < e.length; i++) {
                log.info("original: " + dt[i]);
                log.info("asValue : " + ext.asValue((LiteralExtensionIV<?>) e[i], vf));
                log.info("decoded : " + ext.asValue((LiteralExtensionIV<?>) a[i], vf));
                log.info("");
            }
//          log.info(svf.createLiteral(
//                df.newXMLGregorianCalendar("2001-10-26T21:32:52.12679")));
        }
        
        doComparatorTest(e);
        
    }
    
    
    /**
     * Unit test for {@link SidIV}.
     */
    public void test_encodeDecode_sids() {
        
        final IV<?,?> s1 = newTermId(VTE.URI);
        final IV<?,?> s2 = newTermId(VTE.URI);
        final IV<?,?> p1 = newTermId(VTE.URI);
        final IV<?,?> p2 = newTermId(VTE.URI);
        final IV<?,?> o1 = newTermId(VTE.URI);
        final IV<?,?> o2 = newTermId(VTE.BNODE);
        final IV<?,?> o3 = newTermId(VTE.LITERAL);

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
//        spo1.setStatementIdentifier(true);
//        spo2.setStatementIdentifier(true);
//        spo3.setStatementIdentifier(true);
//        spo6.setStatementIdentifier(true);
        final SPO spo13 = new SPO(spo1.getStatementIdentifier(), p1, o1,
                StatementEnum.Explicit);
        final SPO spo14 = new SPO(spo2.getStatementIdentifier(), p2, o2,
                StatementEnum.Explicit);
        final SPO spo15 = new SPO(s1, p1, spo3.getStatementIdentifier(),
                StatementEnum.Explicit);
//        spo15.setStatementIdentifier(true);
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
     * Unit test for a fully inlined representation of a URI based on a
     * <code>byte</code> code. The flags byte looks like:
     * <code>VTE=URI, inline=true, extension=false,
     * DTE=XSDByte</code>. It is followed by a <code>unsigned byte</code> value
     * which is the index of the URI in the {@link Vocabulary} class for the
     * triple store.
     */
    public void test_encodeDecode_URIByteIV() {

        final IV<?, ?>[] e = {//
                new VocabURIByteIV<BigdataURI>((byte) Byte.MIN_VALUE),//
                new VocabURIByteIV<BigdataURI>((byte) -1),//
                new VocabURIByteIV<BigdataURI>((byte) 0),//
                new VocabURIByteIV<BigdataURI>((byte) 1),//
                new VocabURIByteIV<BigdataURI>((byte) Byte.MAX_VALUE),//
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
                new VocabURIShortIV<BigdataURI>((short) Short.MIN_VALUE),//
                new VocabURIShortIV<BigdataURI>((short) -1),//
                new VocabURIShortIV<BigdataURI>((short) 0),//
                new VocabURIShortIV<BigdataURI>((short) 1),//
                new VocabURIShortIV<BigdataURI>((short) Short.MAX_VALUE),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

}
