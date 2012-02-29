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

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.ServiceFactory;
import com.bigdata.rdf.sparql.ast.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Data driven test suite for SPARQL 1.1 Federated Query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Compute the projected variables. Unless the SERVICE is a within
 *          JVM bigdata aware service, ensure that those variables are
 *          materialized before the SERVICE is invoked in the query plan.
 * 
 *          TODO The ServiceCallJoin operator should be vectored. There are
 *          three cases. Within JVM (bigdata, openrdf) and remote. Transfer of
 *          solutions should be chunked in all cases. For a REMOTE service this
 *          may mean rewriting the service graph pattern into a UNION of graph
 *          patterns having different variables and then reassembling things
 *          afterwards (this is really only a problem for services which have
 *          more than one solution flowing in; multiple solutions flowing out
 *          will be naturally vectored).
 * 
 *          TODO We need to test with a serviceRef which is a URI (done), which
 *          is a value expression which evaluates to a URI, and which is a
 *          variable.
 * 
 *          TODO It might be nice to add a TIMEOUT to the SERVICE call.
 * 
 *          TODO Test queries where the evaluation order might not place the
 *          service first. (How?)
 */
public class TestServiceInternal extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestServiceInternal() {
    }

    /**
     * @param name
     */
    public TestServiceInternal(String name) {
        super(name);
    }

    /**
     * A simple SERVICE query against an INTERNAL service. The service adds in a
     * single solution which restricts the set of solutions for the overall
     * query.
     */
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

        ServiceRegistry.add(serviceURI,
                new MockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-001", // testURI
                    "sparql11-service-001.rq",// queryFileURL
                    "sparql11-service-001.ttl",// dataFileURL
                    "sparql11-service-001.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.remove(serviceURI);
            
        }
        
    }
    
    /**
     * A simple SERVICE query against an INTERNAL service. The service provides
     * three solutions, two of which join with the remainder of the query.
     * <p>
     * Note: Since the SERVICE is not actually doing joins, we wind up with
     * duplicate solutions.
     */
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

        ServiceRegistry.add(serviceURI,
                new MockServiceFactory(serviceSolutions));

        try {

            new TestHelper(//
                    "sparql11-service-002", // testURI
                    "sparql11-service-002.rq",// queryFileURL
                    "sparql11-service-002.ttl",// dataFileURL
                    "sparql11-service-002.srx"// resultFileURL
            ).runTest();
            
        } finally {
            
            ServiceRegistry.remove(serviceURI);
            
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

        private final List<IBindingSet> serviceSolutions;
        
        public MockServiceFactory(final List<IBindingSet> serviceSolutions) {

            this.serviceSolutions = serviceSolutions;
            
        }
        
        @Override
        public BigdataServiceCall create(final AbstractTripleStore store,
                final IGroupNode<IGroupMemberNode> groupNode) {

            assertNotNull(store);
            
            assertNotNull(groupNode);

            return new MockBigdataServiceCall();
            
        }
        
        private class MockBigdataServiceCall implements BigdataServiceCall {

            @Override
            public ICloseableIterator<IBindingSet> call(
                    final IBindingSet[] bindingSets) {

                assertNotNull(bindingSets);

//                System.err.println("ServiceCall: in="+Arrays.toString(bindingSets));
//                
//                System.err.println("ServiceCall: out="+serviceSolutions);
                
                return new CloseableIteratorWrapper<IBindingSet>(
                        serviceSolutions.iterator());

            }
            
        }
        
    }

}
