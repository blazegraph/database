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
import com.bigdata.rdf.sparql.ast.service.ExternalServiceCall;
import com.bigdata.rdf.sparql.ast.service.ExternalServiceOptions;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query against an openrdf
 * aware SERVICE implementation running in the same JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceExternal extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestServiceExternal() {
    }

    /**
     * @param name
     */
    public TestServiceExternal(String name) {
        super(name);
    }

    /**
     * A simple SERVICE query. The service adds in a single solution which
     * restricts the set of solutions for the overall query.
     */
    public void test_service_001() throws Exception {
        
//        final BigdataValueFactory valueFactory = store.getValueFactory();
//        
//        /*
//         * Note: This IV needs to be resolved in order to join against data that
//         * loaded into the database. Since we have not yet loaded the data, the
//         * RDF Value is being inserted into the store now. That way, when the
//         * data are loaded, the data will use the same IV and the join will
//         * succeed.
//         */
//        final BigdataValue book1 = valueFactory
//                .createURI("http://example.org/book/book1");
//        {
//            store.addTerm(book1);
//        }
        
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
                new MockServiceFactory(serviceSolutions));

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
     */
    public void test_service_002() throws Exception {
        
//        final BigdataValueFactory valueFactory = store.getValueFactory();
//        
//        /*
//         * Note: This IV needs to be resolved in order to join against data that
//         * loaded into the database. Since we have not yet loaded the data, the
//         * RDF Value is being inserted into the store now. That way, when the
//         * data are loaded, the data will use the same IV and the join will
//         * succeed.
//         */
//        final BigdataValue book1 = valueFactory
//                .createURI("http://example.org/book/book1");
//        {
//            store.addTerm(book1);
//        }
//
//        final BigdataValue book2 = valueFactory
//                .createURI("http://example.org/book/book2");
//        {
//            store.addTerm(book2);
//        }

        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        {
            final MapBindingSet bset = new MapBindingSet();
//            bset.addBinding("book", book1);
            bset.addBinding("book",
                    new URIImpl("http://example.org/book/book1"));
            serviceSolutions.add(bset);
        }
        {
            final MapBindingSet bset = new MapBindingSet();
//            bset.addBinding("book", book2);
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
                new MockServiceFactory(serviceSolutions));

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
    
    /**
     * Mock service reports the solutions provided in the constructor.
     * <p>
     * Note: This can not be used to test complex queries because the caller
     * needs to know the order in which the query will be evaluated in order to
     * know the correct response for the mock service.
     */
    private static class MockServiceFactory implements ServiceFactory
    {

        private final ExternalServiceOptions serviceOptions = new ExternalServiceOptions();
        
        private final List<BindingSet> serviceSolutions;
        
        public MockServiceFactory(final List<BindingSet> serviceSolutions) {

            this.serviceSolutions = serviceSolutions;
            
        }
        
        @Override
        public ServiceCall<?> create(final AbstractTripleStore store,
                final URI serviceURI, final ServiceNode serviceNode) {

            assertNotNull(store);
            
            assertNotNull(serviceNode);

            return new MockExternalServiceCall();
            
        }
        
        @Override
        public IServiceOptions getServiceOptions() {
            return serviceOptions;
        }

        private class MockExternalServiceCall implements ExternalServiceCall {

            @Override
            public ICloseableIterator<BindingSet> call(
                    final BindingSet[] bindingSets) {

                assertNotNull(bindingSets);
                
//                System.err.println("ServiceCall: in="+Arrays.toString(bindingSets));
//                
//                System.err.println("ServiceCall: out="+serviceSolutions);
                
                return new CloseableIteratorWrapper<BindingSet>(
                        serviceSolutions.iterator());

            }

            @Override
            public IServiceOptions getServiceOptions() {
                return serviceOptions;
            }

        }

    }
    
}
