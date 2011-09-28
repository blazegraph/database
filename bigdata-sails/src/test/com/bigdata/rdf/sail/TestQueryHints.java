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
 * Created on Jun 20, 2011
 */

package com.bigdata.rdf.sail;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.MalformedQueryException;

import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sail.sparql.BigdataSPARQLParser;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.BD;

/**
 * Unit test for {@link BigdataSPARQLParser}'s ability to report the
 * {@link QueryHints} associated with a SPARQL query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestQueryHintsUtility.java 4739 2011-06-20 12:42:13Z
 *          thompsonbry $
 */
public class TestQueryHints extends TestCase2 {

    /**
     * 
     */
    public TestQueryHints() {
    }

    /**
     * @param name
     */
    public TestQueryHints(String name) {
        super(name);
    }

    public void test_selectQuery() throws MalformedQueryException {

        final Map<String,String> expected = new LinkedHashMap<String, String>();
        {

            expected.put(PipelineOp.Annotations.MAX_PARALLEL,"-5");
            
            expected.put("com.bigdata.fullScanTreshold","1000");
            
        }

        final String baseURI = "http://www.bigdata.com/sparql";

        final URI a = new URIImpl("_:A");

        final String qs = //
                "PREFIX " + QueryHints.PREFIX + ": " + //
                "<http://www.bigdata.com/queryOption" + //
                "#" + PipelineOp.Annotations.MAX_PARALLEL + "=-5" + //
                "&" + "com.bigdata.fullScanTreshold=1000" //
                + ">\n"//
                + "SELECT * " + "WHERE { " + "  <" + a + "> ?p ?o " + "}";

        final Properties actual = ((IBigdataParsedQuery) new BigdataSPARQLParser()
                .parseQuery(qs, baseURI)).getQueryHints();

        assertSameProperties(expected, actual);

    }

    public void test_describeQuery() throws MalformedQueryException {

        final Map<String,String> expected = new LinkedHashMap<String, String>();
        {

            expected.put(PipelineOp.Annotations.MAX_PARALLEL,"-5");
            
            expected.put("com.bigdata.fullScanTreshold","1000");
            
        }

        final String baseURI = "http://www.bigdata.com/sparql";

        final String qs = 
            "prefix bd: <"+BD.NAMESPACE+"> " +
            "prefix rdf: <"+RDF.NAMESPACE+"> " +
            "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
            "PREFIX " + QueryHints.PREFIX + ": " + //
            "<http://www.bigdata.com/queryOption" + //
            "#" + PipelineOp.Annotations.MAX_PARALLEL + "=-5" + //
            "&" + "com.bigdata.fullScanTreshold=1000" //
            + ">\n"+//
            "describe ?x " +//
            "WHERE { " +//
            "  ?x rdf:type bd:Person . " +//
            "  ?x bd:likes bd:RDF " +//
            "}";

        final Properties actual = ((IBigdataParsedQuery) new BigdataSPARQLParser()
                .parseQuery(qs, baseURI)).getQueryHints();

        assertSameProperties(expected, actual);

    }

    private static void assertSameProperties(
            final Map<String, String> expected, final Properties actual) {

        assertEquals("size", expected.size(), actual.size());

        for (Map.Entry<String, String> e : expected.entrySet()) {

            final String name = e.getKey();

            final String expectedValue = e.getValue();

            final String actualValue = actual.getProperty(name);

            assertEquals(name, expectedValue, actualValue);

        }
        
    }

}
