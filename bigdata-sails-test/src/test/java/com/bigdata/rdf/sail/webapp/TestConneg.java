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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

import junit.framework.TestCase2;

import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.format.CounterSetFormat;

/**
 * Test suite for content negotiation helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestConneg extends TestCase2 {

	
//	static {
//		
//		new BigdataSPARQLResultsJSONParserFactory();
//		
//	}
	
    /**
     * 
     */
    public TestConneg() {
        
    }

    /**
     * @param name
     */
    public TestConneg(final String name) {

        super(name);
        
    }

    public void test_conneg_no_Accept_headser() {
    
        final String acceptStr = null;

        final ConnegUtil util = new ConnegUtil(acceptStr);

        assertNull(util.getRDFFormat());

        assertEquals(RDFFormat.N3, util.getRDFFormat(RDFFormat.N3));

    }

    public void test_conneg_empty_Accept_headser() {
        
        final String acceptStr = "";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        assertNull(util.getRDFFormat());

        assertEquals(RDFFormat.N3, util.getRDFFormat(RDFFormat.N3));

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

            if (!format.getName().equals("JSON")) {

                assertNull(format.getName(), util.getTupleQueryResultFormat());

            }

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

            if (!format.getName().equals("SPARQL/JSON")) {
                
            	assertNull(format.getName(), util.getRDFFormat());

            }
            
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
                new ConnegScore(.5f, TupleQueryResultFormat.SPARQL),//
                new ConnegScore(.3f, TupleQueryResultFormat.BINARY),//
                },//
                util.getScores(TupleQueryResultFormat.class));

        assertEquals(TupleQueryResultFormat.SPARQL,
                util.getTupleQueryResultFormat());

    }

    public void test_conneg_sparql_result_set_03b() {

        final String acceptStr = "text/xhtml,application/x-binary-rdf-results-table;q=.4,application/sparql-results+xml;q=.2";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        assertNull(util.getRDFFormat());

        assertSameArray(new ConnegScore[] {//
                new ConnegScore(.4f, TupleQueryResultFormat.BINARY),//
                new ConnegScore(.2f, TupleQueryResultFormat.SPARQL),//
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

            if (!format.getName().equals("SPARQL/JSON")) {
                
            	assertNull(format.getName(), util.getRDFFormat());
            	
            }

            assertEquals(format.getName(), format,
                    util.getBooleanQueryResultFormat());

            assertSameArray(new ConnegScore[] {//
                    new ConnegScore(1f, format) },//
                    util.getScores(BooleanQueryResultFormat.class));

        }

    }
    
    public void test_conneg_ask_json() {

        final ConnegUtil util = new ConnegUtil(
                BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON);

        final BooleanQueryResultFormat format = util
                .getBooleanQueryResultFormat(BooleanQueryResultFormat.SPARQL);

        assertFalse(format.toString(), format.toString().toLowerCase()
                .contains("xml"));

    }

    public void test_conneg_counterSet_application_xml() {
        
        final String acceptStr = "application/xml";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        final CounterSetFormat format = util.getCounterSetFormat();

        assertEquals(CounterSetFormat.XML, format);

    }
    
    public void test_conneg_counterSet_text_plain() {
        
        final String acceptStr = "text/plain";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        final CounterSetFormat format = util.getCounterSetFormat();

        assertEquals(CounterSetFormat.TEXT, format);

    }
    
    public void test_conneg_counterSet_text_html() {
        
        final String acceptStr = "text/html";

        final ConnegUtil util = new ConnegUtil(acceptStr);

        final CounterSetFormat format = util.getCounterSetFormat();

        assertEquals(CounterSetFormat.HTML, format);

    }

    public void test_conneg_counterSet_browser1() {

        final String acceptStr = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8,";
        
        final ConnegUtil util = new ConnegUtil(acceptStr);

//        System.out.println(Arrays.toString(util.getScores(CounterSetFormat.class)));
        
        assertSameArray(new ConnegScore[] {//
                new ConnegScore(1f, CounterSetFormat.HTML),//
                new ConnegScore(.9f, CounterSetFormat.XML),//
                },//
                util.getScores(CounterSetFormat.class));

        final CounterSetFormat format = util.getCounterSetFormat();

        assertEquals(CounterSetFormat.HTML, format);

    }
    
	public void test_connect_getMimeTypeForQueryParameter2() {
		final String outputFormat = BigdataRDFServlet.OUTPUT_FORMAT_JSON_SHORT;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON;
		final String acceptHeader = null;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
	
	public void test_connect_getMimeTypeForQueryParameter3() {
		final String outputFormat = BigdataRDFServlet.OUTPUT_FORMAT_XML;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;
		final String acceptHeader = null;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
	
	public void test_connect_getMimeTypeForQueryParameter4() {
		final String outputFormat = BigdataRDFServlet.OUTPUT_FORMAT_XML_SHORT;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;
		final String acceptHeader = null;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
	
	public void test_connect_getMimeTypeForQueryParameter5() {
		final String outputFormat = null;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;
		final String acceptHeader = null;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
	
	public void test_connect_getMimeTypeForQueryParameter6() {
		final String outputFormat = BigdataRDFServlet.OUTPUT_FORMAT_JSON;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON;
		final String acceptHeader = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
	
	public void test_connect_getMimeTypeForQueryParameter7() {
		final String outputFormat = BigdataRDFServlet.OUTPUT_FORMAT_XML;
		final String correctResult = BigdataRDFServlet.MIME_SPARQL_RESULTS_XML;
		final String acceptHeader = BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON;

		assertEquals(ConnegUtil.getMimeTypeForQueryParameterQueryRequest(outputFormat,
				acceptHeader), correctResult);
	}
    
}
