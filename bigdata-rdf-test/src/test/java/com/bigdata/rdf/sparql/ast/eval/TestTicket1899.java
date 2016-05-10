/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;


/**
 * Test suite for a NotMaterializedException caused by HTree hash index built.
 * The test is defined over geospatial data, which gives us LiteralExtensionIVs
 * allowing to reproduce the problem.
 * 
 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1899">NotMaterializedException caused by HTree hash index</a>
 */
public class TestTicket1899 extends AbstractDataDrivenSPARQLTestCase {

    public TestTicket1899() {
    }

    public TestTicket1899(String name) {
        super(name);
    }


    /**
     * Test case for property paths induced hash index.
     */
    public void test_ticket_bg1899_a() throws Exception {
        
        new TestHelper(
                "ticket_bg1899a", // testURI,
                "ticket_bg1899a.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    
    /**
     * Test case for subquery induced hash index.
     */
    public void test_ticket_bg1899_b() throws Exception {
        
        new TestHelper(
                "ticket_bg1899b", // testURI,
                "ticket_bg1899b.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for complex join group induced hash index.
     */
    public void test_ticket_bg1899_c() throws Exception {
        
        new TestHelper(
                "ticket_bg1899c", // testURI,
                "ticket_bg1899c.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for FILTER EXISTS induced hash index.
     */
    public void test_ticket_bg1899_d() throws Exception {
        
        new TestHelper(
                "ticket_bg1899d", // testURI,
                "ticket_bg1899d.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for proper reconstruction of cache from HTree
     * index for URIs.
     */
    public void test_ticket_bg1899_e() throws Exception {
        
        new TestHelper(
                "ticket_bg1899e", // testURI,
                "ticket_bg1899e.rq",// queryFileURL
                "ticket_bg1899e.nt",// dataFileURL
                "ticket_bg1899e.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for proper reconstruction of cache from HTree
     * index for literals.
     */
    public void test_ticket_bg1899_f() throws Exception {
        
        new TestHelper(
                "ticket_bg1899f", // testURI,
                "ticket_bg1899f.rq",// queryFileURL
                "ticket_bg1899f.nt",// dataFileURL
                "ticket_bg1899f.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for proper reconstruction of cache from HTree
     * index for large literals (BLOBS).
     */
    public void test_ticket_bg1899_g() throws Exception {
        
        new TestHelper(
                "ticket_bg1899g", // testURI,
                "ticket_bg1899g.rq",// queryFileURL
                "ticket_bg1899g.nt",// dataFileURL
                "ticket_bg1899g.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for proper reconstruction of cache from HTree
     * index for numerics.
     */
    public void test_ticket_bg1899_h() throws Exception {
        
        new TestHelper(
                "ticket_bg1899h", // testURI,
                "ticket_bg1899h.rq",// queryFileURL
                "ticket_bg1899h.n3",// dataFileURL
                "ticket_bg1899h.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for OPTIONAL join, where we have no materialization
     * guarantees at all.
     */
    public void test_ticket_bg1899_i() throws Exception {
        
        new TestHelper(
                "ticket_bg1899c", // testURI,
                "ticket_bg1899i.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    /**
     * Test case for EXISTS join, where we have no materialization
     * guarantees at all.
     */
    public void test_ticket_bg1899_j() throws Exception {
        
        new TestHelper(
                "ticket_bg1899c", // testURI,
                "ticket_bg1899j.rq",// queryFileURL
                "ticket_bg1899abcd.ttl",// dataFileURL
                "ticket_bg1899abcd.srx"// resultFileURL
                ).runTest();
    }
    
    @Override
    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "false");

        // TM not available with quads.
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE,"false");

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE, BufferMode.Transient.toString());

        // enable GeoSpatial index with default configuration
        properties.setProperty(com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL, "true");

        properties.setProperty(
            com.bigdata.rdf.store.AbstractLocalTripleStore.Options.GEO_SPATIAL_DEFAULT_DATATYPE,
            "http://www.bigdata.com/rdf/geospatial/literals/v1#lat-lon");
        
        return properties;

    }
}
