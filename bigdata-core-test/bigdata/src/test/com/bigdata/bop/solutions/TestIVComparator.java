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
package com.bigdata.bop.solutions;

import java.util.TimeZone;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import junit.framework.TestCase2;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.spo.SPO;

/**
 * Test suite for {@link IVComparator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVComparator extends TestCase2 {

    public TestIVComparator() {
    }

    public TestIVComparator(String name) {
        super(name);
    }

    /**
     * Vocabulary for tests in the outer test suite.
     */
    private static class V {

        final String namespace = "test";

        private long termId = 1L;

        final BigdataValueFactory f = BigdataValueFactoryImpl
                .getInstance(namespace);
        
        /*
         * Literals
         */

        final IV<BigdataLiteral,Void> noninline_plain_lit1 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_plain_lit2 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_languageCode_en_lit1 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_languageCode_en_lit2 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_languageCode_de_lit1 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_languageCode_de_lit2 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_xsd_string_lit1 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        final IV<BigdataLiteral,Void> noninline_xsd_string_lit2 = new TermId<BigdataLiteral>(VTE.LITERAL,termId++);
        
        final IV<BigdataLiteral, Number> inline_xsd_byte1 = new XSDNumericIV<BigdataLiteral>((byte) 1);
        final IV<BigdataLiteral, Number> inline_xsd_int1 = new XSDNumericIV<BigdataLiteral>(1);

        final IV<BigdataLiteral, Boolean> inline_xsd_boolean_true = new XSDBooleanIV<BigdataLiteral>(true);
        final IV<BigdataLiteral, Boolean> inline_xsd_boolean_false = new XSDBooleanIV<BigdataLiteral>(false);

        final IV<BigdataLiteral, Number> inline_xsd_dateTime1 = new XSDNumericIV<BigdataLiteral>(1);

        /*
         * URIs
         */
        final IV<BigdataURI, Void> noninline_uri1 = new TermId<BigdataURI>(VTE.URI, termId++);
        final IV<BigdataURI, Void> noninline_uri2 = new TermId<BigdataURI>(VTE.URI, termId++);
        
        /*
         * Blank nodes.
         */
        final IV<BigdataBNode,Integer> inline_bnode1 = new NumericBNodeIV<BigdataBNode>(1); 
        final IV<BigdataBNode,Integer> inline_bnode2 = new NumericBNodeIV<BigdataBNode>(2); 

        final SidIV<BigdataBNode> sid1 = new SidIV<BigdataBNode>(new SPO(
                noninline_uri1, noninline_uri2, noninline_plain_lit1));

        final SidIV<BigdataBNode> sid2 = new SidIV<BigdataBNode>(new SPO(
                inline_bnode1, noninline_uri2, inline_xsd_dateTime1));

        public V() {

            final DatatypeFactory df;
            try {
                df = DatatypeFactory.newInstance();
            } catch (DatatypeConfigurationException e) {
                throw new RuntimeException(e);
            }
            
            final IDatatypeURIResolver resolver = new IDatatypeURIResolver() {
                public BigdataURI resolve(final URI uri) {
                    final BigdataURI buri = f.createURI(uri.stringValue());
                    buri.setIV(new TermId<BigdataLiteral>(VTE.URI,termId++));
                    return buri;
                }
            };
            
            final DateTimeExtension<BigdataValue> dtExt = new DateTimeExtension<BigdataValue>(
        			resolver, TimeZone.getTimeZone("GMT"));

            dtExt.createIV(f.createLiteral(df
                    .newXMLGregorianCalendar("2001-10-26T21:32:52.126Z")));
            dtExt.createIV(f.createLiteral("2001-10-26", XSD.DATE));
            dtExt.createIV(f.createLiteral("21:32:52.126Z", XSD.TIME));

            noninline_plain_lit1.setValue(f.createLiteral("bigdata"));
            noninline_plain_lit2.setValue(f.createLiteral("systap"));
            noninline_languageCode_en_lit1.setValue(f.createLiteral("bigdata","en"));
            noninline_languageCode_en_lit2.setValue(f.createLiteral("systap","en"));
            noninline_languageCode_de_lit1.setValue(f.createLiteral("bigdata","de"));
            noninline_languageCode_de_lit2.setValue(f.createLiteral("systap","de"));
            noninline_xsd_string_lit1.setValue(f.createLiteral("bigdata", XSD.STRING));
            noninline_xsd_string_lit2.setValue(f.createLiteral("systap", XSD.STRING));
            
            noninline_uri1.setValue(f.createURI("http://www.bigdata.com/"));
            noninline_uri2.setValue(f.createURI("http://www.bigdata.com/blog/"));

        }
        
    }
    
    /**
     * Unit test verifies that an unbound value (a <code>null</code>) is LT
     * anything else.
     */
    public void test_null_lt_anything() {

        final V v = new V();

        final IVComparator c = new IVComparator();

        assertLT(c.compare(null, v.inline_bnode1));

        assertLT(c.compare(null, v.noninline_uri1));

        assertLT(c.compare(null, v.inline_xsd_byte1));

        assertLT(c.compare(null, v.sid1));

    }

    /**
     * Unit test verifies the broad ordering which puts unbound values LT blank
     * nodes LT uris LT literals.
     */
    public void test_null_bnode_uri_literal() {

        final V v = new V();

        final IVComparator c = new IVComparator();

        assertLT(c.compare(null, v.inline_bnode1));
        assertLT(c.compare(null, v.sid2));

        assertLT(c.compare(v.inline_bnode1, v.noninline_uri1));
        assertLT(c.compare(v.sid2, v.noninline_uri1));

        assertLT(c.compare(v.noninline_uri1, v.inline_xsd_byte1));

    }
    
    /**
     * Unit test verifies that things compare equal to themselves.
     */
    public void test_equalToSelf() {

        final V v = new V();

        final IVComparator c = new IVComparator();

        assertEQ(c.compare(null,null));
        
        assertEQ(c.compare(v.sid1, v.sid1));
        
        assertEQ(c.compare(v.inline_bnode1, v.inline_bnode1));
        
        assertEQ(c.compare(v.noninline_uri1, v.noninline_uri1));
        
        assertEQ(c.compare(v.inline_xsd_byte1, v.inline_xsd_byte1));

    }
    
    /**
     * Unit test of the relative ordering of blank nodes (they are ordered by
     * the {@link IV}'s natural order in order to cause the same {@link IV}s to
     * be groups).
     * 
     * @see <a href="http://www.openrdf.org/issues/browse/SES-873"> Order the
     *      same Blank Nodes together in ORDER BY</a>
     */
    public void test_bnode_ordering() {
        
        final V v = new V();

        final IVComparator c = new IVComparator();

        // These are not the same bnode.
        assertNotSame(v.inline_bnode1, v.inline_bnode2);
        assertNotSame(v.sid1, v.sid2);

        // The do not compare as EQ.
        assertTrue(0 != c.compare(v.inline_bnode1, v.inline_bnode2));
        assertTrue(0 != c.compare(v.sid1, v.inline_bnode2));
        assertTrue(0 != c.compare(v.sid1, v.sid2));
        assertTrue(0 != c.compare(v.inline_bnode1, v.sid2));

        if (v.inline_bnode1.compareTo(v.inline_bnode2) < 0) {
            assertLT(c.compare(v.inline_bnode1, v.inline_bnode2));
        } else {
            assertGT(c.compare(v.inline_bnode1, v.inline_bnode2));
        }

        if (v.sid1.compareTo(v.sid2) < 0) {
            assertLT(c.compare(v.sid1, v.sid2));
        } else {
            assertGT(c.compare(v.sid1, v.sid2));
        }

        /*
         * Comparisons between SIDs and blank nodes. Order SIDs after normal
         * blank nodes.
         */
        
//        if (v.inline_bnode1.compareTo(v.sid1) < 0) {
            assertLT(c.compare(v.inline_bnode1, v.sid1));
//        } else {
//            assertGT(c.compare(v.inline_bnode1, v.sid1));
//        }

//        if (v.sid2.compareTo(v.inline_bnode2) < 0) {
//            assertLT(c.compare(v.sid2, v.inline_bnode2));
//        } else {
            assertGT(c.compare(v.sid2, v.inline_bnode2));
//        }

    }
    
    /**
     * Unit test of the relative ordering of URIs.
     */
    public void test_uri_ordering() {

        final V v = new V();

        final IVComparator c = new IVComparator();

        assertLT(c.compare(v.noninline_uri1, v.noninline_uri2));

    }

    /**
     * Unit test of the broad ordering of literals (plain LT language code).
     */
    public void test_literal_ordering_plain_languageCode_datatype() {

        final V v = new V();

        final IVComparator c = new IVComparator();

        // plain LT languageCode
        assertLT(c.compare(v.noninline_plain_lit1, v.noninline_languageCode_de_lit1));

    }
    
    /**
     * Unit test of order among plain literals.
     */
    public void test_plain_literal_ordering() {
     
        final V v = new V();

        final IVComparator c = new IVComparator();

        // lexiographic ordering.
        assertLT(c.compare(v.noninline_plain_lit1, v.noninline_plain_lit2));

    }
    
    /**
     * Unit test of order for language code literals having different language
     * codes. The ordering is lexiographic by language code and then by label
     * within each language code.
     */
    public void test_languageCode_ordering() {
     
        final V v = new V();

        final IVComparator c = new IVComparator();

        // lexiographic ordering by language code.
        assertLT(c.compare(v.noninline_languageCode_de_lit1, v.noninline_languageCode_en_lit1));
        
        // lexiographic ordering by label within language code.
        assertLT(c.compare(v.noninline_languageCode_de_lit1, v.noninline_languageCode_de_lit2));

    }
    
    /**
     * Unit test verifies the order imposed across the different datatypes (but
     * not within those datatypes).
     * <p>
     * Note: openrdf imposes the following type precedence:
     * 
     * <pre>
     *              - simple literal
     *              - numeric
     *              - xsd:boolean
     *              - xsd:dateTime
     *              - xsd:string
     *              - RDF term (equal and unequal only)
     * </pre>
     * However, since in RDF 1.1 simple strings are xsd:string, all strings are sorted first.
     */
    public void test_datatype_ordering() {
        
        final V v = new V();

        final IVComparator c = new IVComparator();

        // plain literal LT numeric
        assertLT(c.compare(v.noninline_languageCode_en_lit1, v.inline_xsd_int1));
        assertLT(c.compare(v.noninline_plain_lit1, v.inline_xsd_int1));

        // numeric LT boolean
        assertLT(c.compare(v.inline_xsd_int1, v.inline_xsd_boolean_true));

//        assertLT(c.compare(v.inline_xsd_int1, v.inline_xsd_dateTime1));
//        assertLT(c.compare(v.inline_xsd_boolean_true, v.inline_xsd_dateTime1));

        // In RDF 1.1, xsd:string is simple string
        assertLT(c.compare(v.noninline_xsd_string_lit1, v.inline_xsd_dateTime1));
        assertLT(c.compare(v.noninline_plain_lit1, v.inline_xsd_dateTime1));

    }

//    /**
//     * Unit test comparing different kinds of inline and non-inline {@link IV}s.
//     */
//    public void test_inline_with_noninline() {
//
//        final V v = new V();
//
//        final IVComparator c = new IVComparator();
//
//        fail("write test");
//        
//    }
    
    private void assertLT(final int ret) {
        assertTrue(ret < 0);
    }

    private void assertGT(final int ret) {
        assertTrue(ret > 0);
    }

    private void assertEQ(final int ret) {
        assertTrue(ret == 0);
    }

}
