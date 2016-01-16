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
 * Created on Oct 4, 2011
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

import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.internal.ColorsEnumExtension.Color;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.bnode.FullyInlineUnicodeBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.extensions.DerivedNumericsExtension;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.PartlyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
import com.bigdata.rdf.internal.impl.uri.FullyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.PartlyInlineURIIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.internal.impl.uri.VocabURIShortIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test of encode/decode and especially <em>comparator</em> semantics for mixed
 * {@link IV}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractEncodeDecodeMixedIVsTest extends AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public AbstractEncodeDecodeMixedIVsTest() {
    }

    /**
     * @param name
     */
    public AbstractEncodeDecodeMixedIVsTest(String name) {
        super(name);
    }

    /**
     * Flag may be used to enable/disable the inclusion of the {@link IV}s
     * having fully include Unicode data. These are the ones whose proper
     * ordering is most problematic as they need to obey the collation order
     * imposed by the {@link AbstractTripleStore.Options}.
     */
    static private boolean fullyInlineUnicode = true;
    
    protected List<IV<?,?>> prepareIVs() throws DatatypeConfigurationException {
        
        final Random r = new Random();

        final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance(getName());
        
        final URI datatype = new URIImpl("http://www.bigdata.com");
        final URI dt1 = new URIImpl("http://www.bigdata.com/mock-datatype-1");
        final URI dt2 = new URIImpl("http://www.bigdata.com/mock-datatype-2");

        final IV<?, ?> namespaceIV = newTermId(VTE.URI);
        final IV<?, ?> datatypeIV = newTermId(VTE.URI);
        final IV<?, ?> datatypeIV2 = newTermId(VTE.URI);

        final IV<?, ?> colorIV = newTermId(VTE.URI);// ColorsEnumExtension.COLOR;
        final IV<?, ?> xsdStringIV = newTermId(VTE.URI);// XSD.STRING;
        final IV<?, ?> xsdDateTimeIV = newTermId(VTE.URI);// XSD.DATETIME;

        final IDatatypeURIResolver resolver = new IDatatypeURIResolver() {
            public BigdataURI resolve(final URI uri) {
                final BigdataURI buri = vf.createURI(uri.stringValue());
                if (ColorsEnumExtension.COLOR.equals(uri)) {
                    buri.setIV(colorIV);
                } else if (XSD.STRING.equals(uri)) {
                    buri.setIV(xsdStringIV);
                } else if (XSD.DATETIME.equals(uri)) {
                    buri.setIV(xsdDateTimeIV);
                } else if (XSD.DATE.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.TIME.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.GDAY.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.GMONTH.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.GMONTHDAY.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.GYEAR.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.GYEARMONTH.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.POSITIVE_INTEGER.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.NEGATIVE_INTEGER.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.NON_POSITIVE_INTEGER.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else if (XSD.NON_NEGATIVE_INTEGER.equals(uri)) {
                    buri.setIV(newTermId(VTE.URI));
                } else
                    throw new UnsupportedOperationException();
                return buri;
            }
        };
       
        final List<IV<?,?>> ivs = new LinkedList<IV<?,?>>();
        {

            // Fully inline
            {

                /*
                 * BNODEs
                 */
                if (fullyInlineUnicode) {
                    // blank nodes with Unicode IDs.
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("FOO"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("_bar"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("bar"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("baz"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("12"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("1298"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("asassdao"));
                    ivs.add(new FullyInlineUnicodeBNodeIV<BigdataBNode>("1"));
                }

                // blank nodes with numeric IDs.
                ivs.add(new NumericBNodeIV<BigdataBNode>(-1));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(0));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(1));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(-52));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(52));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(Integer.MAX_VALUE));//
                ivs.add(new NumericBNodeIV<BigdataBNode>(Integer.MIN_VALUE));//

                // blank nodes with UUID IDs.
                for (int i = 0; i < 100; i++) {

                    ivs.add(new UUIDBNodeIV<BigdataBNode>(UUID.randomUUID()));

                }

                /*
                 * URIs
                 */
                ivs.add(new FullyInlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com")));
                ivs.add(new FullyInlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com/")));
                ivs.add(new FullyInlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com/foo")));
                ivs.add(new FullyInlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com:80/foo")));
                ivs.add(new FullyInlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com")));
                if (fullyInlineUnicode) {
                    ivs.add(new FullyInlineURIIV<BigdataURI>(RDF.TYPE));
                    ivs.add(new FullyInlineURIIV<BigdataURI>(RDF.SUBJECT));
                    ivs.add(new FullyInlineURIIV<BigdataURI>(RDF.BAG));
                    ivs.add(new FullyInlineURIIV<BigdataURI>(RDF.OBJECT));
                    ivs.add(new URIExtensionIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>(
                                    "http://www.example.com/"),
                            new VocabURIByteIV<BigdataURI>((byte) 1)));
                    ivs.add(new URIExtensionIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>(
                                    "http://www.example.com/foo"),
                            new VocabURIByteIV<BigdataURI>((byte) 1)));
                    ivs.add(new URIExtensionIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>(
                                    "http://www.example.com/foobar"),
                            new VocabURIByteIV<BigdataURI>((byte) 1)));
                    }

                /*
                 * Literals
                 */
                
                if (fullyInlineUnicode) {
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "foo", null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "bar", null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "baz", null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "123", null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("23",
                            null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("3",
                            null/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("",
                            null/* language */, null/* datatype */));
                }

                if (fullyInlineUnicode) {
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "foo", "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "bar", "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "goo", "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "baz", "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "foo", "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "bar", "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "goo", "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "baz", "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("",
                            "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1",
                            "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12",
                            "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("2",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("2",
                            "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("23",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("23",
                            "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "123", "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "123", "de"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("3",
                            "en"/* language */, null/* datatype */));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("3",
                            "de"/* language */, null/* datatype */));
                }

                if (fullyInlineUnicode) {
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "foo", null/* language */, dt1));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "bar", null/* language */, dt1));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "baz", null/* language */, dt1));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "goo", null/* language */, dt1));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "foo", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "bar", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "baz", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "goo", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "123", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(
                            "123", null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("23",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("23",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("3",
                            null/* language */, dt2));
                    ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("3",
                            null/* language */, dt2));
                }

                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("foo",
                        null/* language */, XSD.STRING/* datatype */));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("bar",
                        null/* language */, XSD.STRING/* datatype */));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("baz",
                        null/* language */, XSD.STRING/* datatype */));

                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(""));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" "));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1"));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12"));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123"));
                
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("","en",null/*datatype*/));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ","en",null/*datatype*/));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1","en",null/*datatype*/));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12","fr",null/*datatype*/));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123","de",null/*datatype*/));
    
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("", null, datatype));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ", null, datatype));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1", null, datatype));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12", null, datatype));
                ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123", null, datatype));

                // xsd:boolean
                ivs.add(new XSDBooleanIV<BigdataLiteral>(true));//
                ivs.add(new XSDBooleanIV<BigdataLiteral>(false));//

                // xsd:byte
                ivs.add(new XSDNumericIV<BigdataLiteral>((byte)Byte.MIN_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>((byte)-1));
                ivs.add(new XSDNumericIV<BigdataLiteral>((byte)0));
                ivs.add(new XSDNumericIV<BigdataLiteral>((byte)1));
                ivs.add(new XSDNumericIV<BigdataLiteral>((byte)Byte.MAX_VALUE));

                // xsd:short
                ivs.add(new XSDNumericIV<BigdataLiteral>((short)-1));
                ivs.add(new XSDNumericIV<BigdataLiteral>((short)0));
                ivs.add(new XSDNumericIV<BigdataLiteral>((short)1));
                ivs.add(new XSDNumericIV<BigdataLiteral>((short)Short.MIN_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>((short)Short.MAX_VALUE));
                
                // xsd:int
                ivs.add(new XSDNumericIV<BigdataLiteral>(1));
                ivs.add(new XSDNumericIV<BigdataLiteral>(0));
                ivs.add(new XSDNumericIV<BigdataLiteral>(-1));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Integer.MAX_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Integer.MIN_VALUE));

                // xsd:long
                ivs.add(new XSDNumericIV<BigdataLiteral>(1L));
                ivs.add(new XSDNumericIV<BigdataLiteral>(0L));
                ivs.add(new XSDNumericIV<BigdataLiteral>(-1L));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Long.MIN_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Long.MAX_VALUE));

                // xsd:float
                ivs.add(new XSDNumericIV<BigdataLiteral>(1f));
                ivs.add(new XSDNumericIV<BigdataLiteral>(-1f));
                ivs.add(new XSDNumericIV<BigdataLiteral>(+0f));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.MAX_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.MIN_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.MIN_NORMAL));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.POSITIVE_INFINITY));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.NEGATIVE_INFINITY));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Float.NaN));
                
                // xsd:double
                ivs.add(new XSDNumericIV<BigdataLiteral>(1d));
                ivs.add(new XSDNumericIV<BigdataLiteral>(-1d));
                ivs.add(new XSDNumericIV<BigdataLiteral>(+0d));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.MAX_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.MIN_VALUE));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.MIN_NORMAL));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.POSITIVE_INFINITY));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.NEGATIVE_INFINITY));
                ivs.add(new XSDNumericIV<BigdataLiteral>(Double.NaN));
                
                // uuid (not an official xsd type, but one we handle natively).
                for (int i = 0; i < 100; i++) {
                    ivs.add(new UUIDLiteralIV<BigdataLiteral>(UUID.randomUUID()));
                }
                
                // xsd:unsignedByte
                ivs.add(new XSDUnsignedByteIV<BigdataLiteral>(Byte.MIN_VALUE));
                ivs.add(new XSDUnsignedByteIV<BigdataLiteral>((byte) -1));
                ivs.add(new XSDUnsignedByteIV<BigdataLiteral>((byte) 0));
                ivs.add(new XSDUnsignedByteIV<BigdataLiteral>((byte) 1));
                ivs.add(new XSDUnsignedByteIV<BigdataLiteral>(Byte.MAX_VALUE));

                // xsd:unsignedShort
                ivs.add(new XSDUnsignedShortIV<BigdataLiteral>(Short.MIN_VALUE));
                ivs.add(new XSDUnsignedShortIV<BigdataLiteral>((short) -1));
                ivs.add(new XSDUnsignedShortIV<BigdataLiteral>((short) 0));
                ivs.add(new XSDUnsignedShortIV<BigdataLiteral>((short) 1));
                ivs.add(new XSDUnsignedShortIV<BigdataLiteral>(Short.MAX_VALUE));

                // xsd:unsignedInt
                ivs.add(new XSDUnsignedIntIV<BigdataLiteral>(Integer.MIN_VALUE));
                ivs.add(new XSDUnsignedIntIV<BigdataLiteral>(-1));
                ivs.add(new XSDUnsignedIntIV<BigdataLiteral>(0));
                ivs.add(new XSDUnsignedIntIV<BigdataLiteral>(1));
                ivs.add(new XSDUnsignedIntIV<BigdataLiteral>(Integer.MAX_VALUE));

                // xsd:unsignedLong
                ivs.add(new XSDUnsignedLongIV<BigdataLiteral>(Long.MIN_VALUE));
                ivs.add(new XSDUnsignedLongIV<BigdataLiteral>(-1L));
                ivs.add(new XSDUnsignedLongIV<BigdataLiteral>(0L));
                ivs.add(new XSDUnsignedLongIV<BigdataLiteral>(1L));
                ivs.add(new XSDUnsignedLongIV<BigdataLiteral>(Long.MAX_VALUE));

                // xsd:integer
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(-1L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(0L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MAX_VALUE)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MIN_VALUE)));//
    
                // xsd:decimal
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(2.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-2.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(10.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(11.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(258.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(3.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.01)));
                ivs.add(new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.02)));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(new BigDecimal("1.5")));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(new BigDecimal("1.51")));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(new BigDecimal("-1.5")));//
                ivs.add(new XSDDecimalIV<BigdataLiteral>(new BigDecimal("-1.51")));//
                
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(-1L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(0L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1L)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MAX_VALUE)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MIN_VALUE)));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(new BigInteger("15")));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(new BigInteger("151")));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(new BigInteger("-15")));//
                ivs.add(new XSDIntegerIV<BigdataLiteral>(new BigInteger("-151")));//

                // byte vocabulary IVs.
                ivs.add(new VocabURIByteIV<BigdataURI>((byte) Byte.MIN_VALUE));
                ivs.add(new VocabURIByteIV<BigdataURI>((byte) -1));
                ivs.add(new VocabURIByteIV<BigdataURI>((byte) 0));
                ivs.add(new VocabURIByteIV<BigdataURI>((byte) 1));
                ivs.add(new VocabURIByteIV<BigdataURI>((byte) Byte.MAX_VALUE));

                // short vocabulary IVs.
                ivs.add(new VocabURIShortIV<BigdataURI>((short) Short.MIN_VALUE));
                ivs.add(new VocabURIShortIV<BigdataURI>((short) -1));
                ivs.add(new VocabURIShortIV<BigdataURI>((short) 0));
                ivs.add(new VocabURIShortIV<BigdataURI>((short) 1));
                ivs.add(new VocabURIShortIV<BigdataURI>((short) Short.MAX_VALUE));

                // SIDs
                {
                    
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
//                    spo1.setStatementIdentifier(true);
//                    spo2.setStatementIdentifier(true);
//                    spo3.setStatementIdentifier(true);
//                    spo6.setStatementIdentifier(true);
                    final SPO spo13 = new SPO(spo1.getStatementIdentifier(), p1, o1,
                            StatementEnum.Explicit);
                    final SPO spo14 = new SPO(spo2.getStatementIdentifier(), p2, o2,
                            StatementEnum.Explicit);
                    final SPO spo15 = new SPO(s1, p1, spo3.getStatementIdentifier(),
                            StatementEnum.Explicit);
//                    spo15.setStatementIdentifier(true);
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
                    ivs.addAll(Arrays.asList(e));
                }
            }

            // Not inline
            {

                /*
                 * TermIds
                 */
                for (int i = 0; i < 100; i++) {

                    for (VTE vte : VTE.values()) {

//                        // 64 bit random term identifier.
//                        final long termId = r.nextLong();
//
//                        final TermId<?> v = new TermId<BigdataValue>(vte,
//                                termId);
//
//                        ivs.add(v);
                        
                        ivs.add(newTermId(vte));

                    }

                }

                /*
                 * BLOBS
                 */
                {

                    for (int i = 0; i < 100; i++) {

                        for (VTE vte : VTE.values()) {

                            final int hashCode = r.nextInt();

                            final int counter = Short.MAX_VALUE
                                    - r.nextInt(2 ^ 16);

                            @SuppressWarnings("rawtypes")
                            final BlobIV<?> v = new BlobIV(vte, hashCode,
                                    (short) counter);

                            ivs.add(v);

                        }

                    }

                }

            } // Not inline.
            
            /*
             * Partly inline
             */
            {

                // URIs
                if (fullyInlineUnicode) {
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),
                            namespaceIV));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),
                            namespaceIV));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),
                            namespaceIV));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),
                            namespaceIV));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),
                            namespaceIV));
                }

                // LITERALs
                ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                        new FullyInlineTypedLiteralIV<BigdataLiteral>(""),
                        datatypeIV));

                ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                        new FullyInlineTypedLiteralIV<BigdataLiteral>("abc"),
                        datatypeIV));

                ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                        new FullyInlineTypedLiteralIV<BigdataLiteral>(" "),
                        datatypeIV));

                ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                        new FullyInlineTypedLiteralIV<BigdataLiteral>("1"),
                        datatypeIV));

                ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                        new FullyInlineTypedLiteralIV<BigdataLiteral>("12"),
                        datatypeIV));

                if (fullyInlineUnicode) {
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>(""),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>(" "),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("1"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("1"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("12"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("12"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),
                            datatypeIV));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),
                            datatypeIV2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),
                            datatypeIV2));
                }

                if(fullyInlineUnicode){
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 1) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 1) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                    ivs.add(new PartlyInlineURIIV<BigdataURI>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),// localName
                            new VocabURIShortIV<BigdataURI>((short) 2) // namespace
                    ));
                }

                if (fullyInlineUnicode) {

                    final IV<?, ?> datatypeIVa = new VocabURIShortIV<BigdataURI>(
                            (short) 1);
                    final IV<?, ?> datatypeIVa2 = new VocabURIShortIV<BigdataURI>(
                            (short) 2);
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),
                            datatypeIVa));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("bar"),
                            datatypeIVa2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),
                            datatypeIVa));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("baz"),
                            datatypeIVa2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),
                            datatypeIVa));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("123"),
                            datatypeIVa2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),
                            datatypeIVa));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("23"),
                            datatypeIVa2));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),
                            datatypeIVa));
                    ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                            new FullyInlineTypedLiteralIV<BigdataLiteral>("3"),
                            datatypeIVa2));

                }

            } // partly inline.
            
            /*
             * Extension IVs
             */
            {

                // xsd:dateTime extension
                {

                    final DatatypeFactory df = DatatypeFactory.newInstance();

                    final DateTimeExtension<BigdataValue> ext = new DateTimeExtension<BigdataValue>(
                            resolver, TimeZone.getDefault());

                    final BigdataLiteral[] dt = {
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("2001-10-26T21:32:52")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("2001-10-26T21:32:52+02:00")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("2001-10-26T19:32:52Z")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("2001-10-26T19:32:52+00:00")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("-2001-10-26T21:32:52")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("2001-10-26T21:32:52.12679")),
                            vf.createLiteral(df
                                    .newXMLGregorianCalendar("1901-10-26T21:32:52")), };

                    for (int i = 0; i < dt.length; i++) {

                        ivs.add(ext.createIV(dt[i]));

                    }

                }

                // derived numerics extension
                {

                    final DatatypeFactory df = DatatypeFactory.newInstance();

                    final DerivedNumericsExtension<BigdataValue> ext = 
                        new DerivedNumericsExtension<BigdataValue>(resolver);
                    
                    final BigdataLiteral[] dt = {
                		vf.createLiteral("1", XSD.POSITIVE_INTEGER),
                		vf.createLiteral("-1", XSD.NEGATIVE_INTEGER),
                		vf.createLiteral("-1", XSD.NON_POSITIVE_INTEGER),
                		vf.createLiteral("1", XSD.NON_NEGATIVE_INTEGER),
                		vf.createLiteral("0", XSD.NON_POSITIVE_INTEGER),
                		vf.createLiteral("0", XSD.NON_NEGATIVE_INTEGER),
                    		};

                    for (int i = 0; i < dt.length; i++) {

                        ivs.add(ext.createIV(dt[i]));

                    }

                }

                // xsd:string extension IVs
                if (fullyInlineUnicode) {

                    final int maxInlineStringLength = 128;

                    final XSDStringExtension<BigdataValue> ext = new XSDStringExtension<BigdataValue>(
                            resolver, maxInlineStringLength);

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
                    ivs.addAll(Arrays.asList(e));
                }

                // "color" extension IV.
                if (true) {

                    final ColorsEnumExtension<BigdataValue> ext = new ColorsEnumExtension<BigdataValue>(
                            resolver);

                    for (Color c : ColorsEnumExtension.Color.values()) {
                     
                        ivs.add(ext.createIV(new LiteralImpl(c.name(),
                                ColorsEnumExtension.COLOR)));
                    }

                }
                
            }

        }
        
        return ivs;

    }
    
}
