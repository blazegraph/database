/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 17, 2011
 */

package com.bigdata.rdf.internal;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

/**
 * Unit tests for {@link IV}s which inline Unicode data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEncodeDecodeUnicodeIVs extends
        AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestEncodeDecodeUnicodeIVs() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeUnicodeIVs(String name) {
        super(name);
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
                new InlineLiteralIV<BigdataLiteral>("2", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("2", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("23", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("23", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("123", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("123", "de"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("3", "en"/* language */,
                        null/* datatype */),//
                new InlineLiteralIV<BigdataLiteral>("3", "de"/* language */,
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
                new InlineLiteralIV<BigdataLiteral>("123", null/* language */, dt2),//
                new InlineLiteralIV<BigdataLiteral>("123", null/* language */, dt2),//
                new InlineLiteralIV<BigdataLiteral>("23", null/* language */, dt2),//
                new InlineLiteralIV<BigdataLiteral>("23", null/* language */, dt2),//
                new InlineLiteralIV<BigdataLiteral>("3", null/* language */, dt2),//
                new InlineLiteralIV<BigdataLiteral>("3", null/* language */, dt2),//
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
     * Test for a URI broken down into namespace and local name components. The
     * namespace component is coded by setting the extension bit and placing the
     * IV of the namespace into the extension IV field. The local name is
     * inlined as a Unicode component using {@link DTE#XSDString}.
     */
    public void test_encodeDecode_NonInline_URI_with_NamespaceIV() {

        final IV<?,?> namespaceIV = newTermId(VTE.URI);
        
        final IV<?, ?>[] e = {//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("bar"), namespaceIV),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("baz"), namespaceIV),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("123"), namespaceIV),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("23"), namespaceIV),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("3"), namespaceIV),//
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

        final IV<?,?> datatypeIV = newTermId(VTE.URI);
        final IV<?,?> datatypeIV2 = newTermId(VTE.URI);

        final IV<?, ?>[] e = {//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>(""), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>(" "), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("1"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("1"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("12"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("12"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("123"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("123"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("23"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("23"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("3"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("3"), datatypeIV2),//
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
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("123"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("123"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("23"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("23"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("3"),// localName
                        new URIShortIV<BigdataURI>((short) 2) // namespace
                ),//
                new URINamespaceIV<BigdataURI>(
                        new InlineLiteralIV<BigdataLiteral>("3"),// localName
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

        final IV<?,?> datatypeIV = new URIShortIV<BigdataURI>((short) 1);
        final IV<?,?> datatypeIV2 = new URIShortIV<BigdataURI>((short) 2);

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
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("23"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("23"), datatypeIV2),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("3"), datatypeIV),//
                new LiteralDatatypeIV<BigdataLiteral>(
                        new InlineLiteralIV<BigdataLiteral>("3"), datatypeIV2),//
        };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

}
