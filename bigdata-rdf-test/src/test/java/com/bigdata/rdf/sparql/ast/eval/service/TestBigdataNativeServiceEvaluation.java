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

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query against an internal,
 * bigdata "aware" service (similar to our integrated full text search
 * facility).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestServiceInternal.java 6053 2012-02-29 18:47:54Z thompsonbry
 *          $
 * 
 *          TODO Write test which uses a variable for the service reference, but
 *          where the service always resolves to a known service. Verify
 *          evaluation with an empty solution in. Maybe write an alternative
 *          test which does the same thing with multiple source solutions in
 *          (e.g., using BINDINGS in the SPARQL query).
 */
public class TestBigdataNativeServiceEvaluation extends
        AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBigdataNativeServiceEvaluation() {
    }

    /**
     * @param name
     */
    public TestBigdataNativeServiceEvaluation(String name) {
        super(name);
    }

    /**
     * A simple SERVICE query against an INTERNAL service. The service adds in a
     * single solution which restricts the set of solutions for the overall
     * query.
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
    @SuppressWarnings("rawtypes")
    public void test_service_001() throws Exception {
        
        /*
         * Note: This IV needs to be resolved in order to join against data that
         * loaded into the database. Since we have not yet loaded the data, the
         * RDF Value is being inserted into the store now. That way, when the
         * data are loaded, the data will use the same IV and the join will
         * succeed.
         */
        final IV<?, ?> book1 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book1"));
        
//        final IV<?,?> fourtyTwo;
////      fourtyTwo = makeIV(store.getValueFactory().createLiteral("42", XSD.INTEGER));
//        fourtyTwo = new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(42));

        final List<IBindingSet> serviceSolutions = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book1));
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.getInstance().add(serviceURI,
                new BigdataNativeMockServiceFactory(serviceSolutions));

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
     * A simple SERVICE query against an INTERNAL service. The service provides
     * three solutions, two of which join with the remainder of the query.
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
    @SuppressWarnings("rawtypes")
    public void test_service_002() throws Exception {
        
        /*
         * Note: This IV needs to be resolved in order to join against data that
         * loaded into the database. Since we have not yet loaded the data, the
         * RDF Value is being inserted into the store now. That way, when the
         * data are loaded, the data will use the same IV and the join will
         * succeed.
         */
        final IV<?, ?> book1 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book1"));
        final IV<?, ?> book2 = store.addTerm(store.getValueFactory().createURI(
                "http://example.org/book/book2"));

        final List<IBindingSet> serviceSolutions = new LinkedList<IBindingSet>();
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book1));
            serviceSolutions.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            bset.set(Var.var("book"), new Constant<IV>(book2));
            serviceSolutions.add(bset);
        }
        {
            final IBindingSet bset = new ListBindingSet();
            serviceSolutions.add(bset);
        }
   
        final URI serviceURI = new URIImpl(
                "http://www.bigdata.com/mockService/" + getName());

        ServiceRegistry.getInstance().add(serviceURI,
                new BigdataNativeMockServiceFactory(serviceSolutions));

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
