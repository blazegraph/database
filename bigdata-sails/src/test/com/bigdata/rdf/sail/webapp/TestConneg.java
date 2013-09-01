/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

import junit.framework.TestCase2;

import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

/**
 * Test suite for content negotiation helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConneg extends TestCase2 {

    /**
     * 
     */
    public TestConneg() {
    }

    /**
     * @param name
     */
    public TestConneg(String name) {
        super(name);
    }
    
    /*
     * Tests where the best format is an RDF data interchange format.
     */

    /** Test the default mime type for each {@link RDFFormat}. */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_conneg_rdf_data_01() {

        for (RDFFormat format : RDFFormat.values()) {

            final ConnegUtil util = new ConnegUtil(format.getDefaultMIMEType());

            assertEquals(format.getName(), format, util.getRDFFormat());

            assertNull(format.getName(), util.getTupleQueryResultFormat());

            assertSameArray(new ConnegScore[] {//
                    new ConnegScore(1f, format) },//
                    util.getScores(RDFFormat.class));

        }

    }

    /*
     * Tests where the best format is a SPARQL result set interchange format.
     */

    /** Test the default mime type for each {@link TupleQueryResultFormat}. */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_conneg_sparql_result_set_01() {

        for (TupleQueryResultFormat format : TupleQueryResultFormat.values()) {

            final ConnegUtil util = new ConnegUtil(format.getDefaultMIMEType());

            assertNull(format.getName(), util.getRDFFormat());

            assertEquals(format.getName(), format,
                    util.getTupleQueryResultFormat());

            assertSameArray(new ConnegScore[] {//
                    new ConnegScore(1f, format) },//
                    util.getScores(TupleQueryResultFormat.class));

        }

    }

    /**
     * Test with multiple values in the accept header.
     */
    public void test_conneg_sparql_result_set_02() {

        final String acceptStr = "application/x-binary-rdf-results-table;q=1,application/sparql-results+xml;q=1";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        assertNull(util.getRDFFormat());

        assertSameArray(new ConnegScore[] {//
                new ConnegScore(1f, TupleQueryResultFormat.BINARY),//
                new ConnegScore(1f, TupleQueryResultFormat.SPARQL),//
                },//
                util.getScores(TupleQueryResultFormat.class));

        assertEquals(TupleQueryResultFormat.BINARY,
                util.getTupleQueryResultFormat());

    }

    public void test_conneg_sparql_result_set_03() {

        final String acceptStr = "text/xhtml,application/x-binary-rdf-results-table;q=.3,application/sparql-results+xml;q=.5";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        assertNull(util.getRDFFormat());

        assertSameArray(new ConnegScore[] {//
                new ConnegScore(.3f, TupleQueryResultFormat.BINARY),//
                new ConnegScore(.5f, TupleQueryResultFormat.SPARQL),//
                },//
                util.getScores(TupleQueryResultFormat.class));

        assertEquals(TupleQueryResultFormat.BINARY,
                util.getTupleQueryResultFormat());

    }

    /** Test the default mime type for each {@link BooleanQueryResultFormat}. */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_conneg_sparql_boolean_result_set_01() {

        for (BooleanQueryResultFormat format : BooleanQueryResultFormat.values()) {

            final ConnegUtil util = new ConnegUtil(format.getDefaultMIMEType());

            assertNull(format.getName(), util.getRDFFormat());

            assertEquals(format.getName(), format,
                    util.getBooleanQueryResultFormat());

            assertSameArray(new ConnegScore[] {//
                    new ConnegScore(1f, format) },//
                    util.getScores(BooleanQueryResultFormat.class));

        }

    }
    
    public void test_conneg_ask_json() {
    	final ConnegUtil util = new ConnegUtil(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON);
    	final BooleanQueryResultFormat format = util
                .getBooleanQueryResultFormat(BooleanQueryResultFormat.SPARQL);
    	assertFalse(format.toString(),format.toString().toLowerCase().contains("xml"));
    }

}
