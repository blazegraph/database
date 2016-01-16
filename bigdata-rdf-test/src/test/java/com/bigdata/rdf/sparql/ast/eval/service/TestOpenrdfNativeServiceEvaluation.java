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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query against an openrdf
 * aware SERVICE implementation running in the same JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOpenrdfNativeServiceEvaluation extends
        AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestOpenrdfNativeServiceEvaluation() {
    }

    /**
     * @param name
     */
    public TestOpenrdfNativeServiceEvaluation(String name) {
        super(name);
    }

    /**
     * A simple SERVICE query. The service adds in a single solution which
     * restricts the set of solutions for the overall query.
     * 
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?book ?title ?price
     * {
     *    SERVICE <http://www.bigdata.com/mockService/test_service_001> {
     *        ?book :foo :bar
     *    }.
     *    ?book dc:title ?title ;
     *          ns:price ?price .
     * }
     * </pre>
     */
    public void test_service_001() throws Exception {
 
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new URIImpl("http://example.org/book/book1"));
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.getInstance().add(serviceURI,
                new OpenrdfNativeMockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-001", // testURI
                    "sparql11-service-001.rq",// queryFileURL
                    "sparql11-service-001.ttl",// dataFileURL
                    "sparql11-service-001.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.getInstance().remove(serviceURI);
            
        }
        
    }
    
    /**
     * A simple SERVICE query. The service provides three solutions, two of
     * which join with the remainder of the query.
     * <p>
     * Note: Since the SERVICE is not actually doing joins, we wind up with
     * duplicate solutions.
     * 
     * <pre>
     * PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
     * PREFIX :     <http://example.org/book/> 
     * PREFIX ns:   <http://example.org/ns#> 
     * 
     * SELECT ?book ?title ?price
     * {
     *    SERVICE <http://www.bigdata.com/mockService/test_service_002> {
     *        ?book :foo :bar
     *    }.
     *    hint:Prior hint:runFirst true .
     *    ?book dc:title ?title ;
     *          ns:price ?price .
     * }
     * </pre>
     */
    public void test_service_002() throws Exception {
 
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new URIImpl("http://example.org/book/book1"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            bset.addBinding("book",
                    new URIImpl("http://example.org/book/book2"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.getInstance().add(serviceURI,
                new OpenrdfNativeMockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-002", // testURI
                    "sparql11-service-002.rq",// queryFileURL
                    "sparql11-service-002.ttl",// dataFileURL
                    "sparql11-service-002.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.getInstance().remove(serviceURI);
            
        }
        
    }
    
}
