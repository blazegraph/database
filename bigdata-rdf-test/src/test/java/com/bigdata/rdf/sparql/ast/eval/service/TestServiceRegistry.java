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
package com.bigdata.rdf.sparql.ast.eval.service;

import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailException;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.AbstractBigdataExprBuilderTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.eval.CustomServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.CustomServiceFactory;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.OpenrdfNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceFactoryImpl;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for the {@link ServiceRegistry}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceRegistry extends AbstractBigdataExprBuilderTestCase {

    private static final Logger log = Logger
            .getLogger(TestServiceRegistry.class);

    public TestServiceRegistry() {
        
    }
    
    public TestServiceRegistry(final String name) {
        super(name);
    }

    /**
     * Overriden: test must be executed in triples mode (test_customService)
     */
    @Override
    protected Properties getProperties() {

       final Properties properties = new Properties();

       // override the default vocabulary.
       properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
               NoVocabulary.class.getName());

       // turn off axioms.
       properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
               NoAxioms.class.getName());

       // Note: No persistence.
       properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
               BufferMode.Transient.toString());
       
       return properties;

   }    
    
    /**
     * Unit test for adding, resolving, and removing a {@link ServiceFactory}.
     */
    public void test_addGetRemove() {

        final URI serviceURI1 = new URIImpl("http://www.bigdata.com/myService/"
                + getName() + "/" + UUID.randomUUID());

        final URI serviceURI2 = new URIImpl(
                "http://www.bigdata.com/myService2/" + getName() + "/"
                        + UUID.randomUUID());

        final RemoteServiceOptions options = new RemoteServiceOptions();

        final RemoteServiceFactoryImpl serviceFactory = new RemoteServiceFactoryImpl(
                options);

        try {

            // Verify not registered.
            assertNull(ServiceRegistry.getInstance().get(serviceURI1));
            assertNull(ServiceRegistry.getInstance().get(serviceURI2));

            // Register.
            ServiceRegistry.getInstance().add(serviceURI1, serviceFactory);

            // Verify discovery.
            assertNotNull(ServiceRegistry.getInstance().get(serviceURI1));
            assertNull(ServiceRegistry.getInstance().get(serviceURI2));

            // Verify same object.
            assertTrue(serviceFactory == ServiceRegistry.getInstance().get(
                    serviceURI1));

        } finally {

            // De-register.
            ServiceRegistry.getInstance().remove(serviceURI1);

        }

        // Verify not registered.
        assertNull(ServiceRegistry.getInstance().get(serviceURI1));
        assertNull(ServiceRegistry.getInstance().get(serviceURI2));

    }

    /**
     * Unit test service alias.
     */
    public void test_serviceAlias() {

        /*
         * Note: Avoid possible side-effects from uncleared service registries
         * using a UUID to make these URIs distinct from others that might be in
         * the ServiceRegistry.
         */
        final URI serviceURI1 = new URIImpl("http://www.bigdata.com/myService/"
                + getName() + "/" + UUID.randomUUID());

        final URI serviceURI2 = new URIImpl(
                "http://www.bigdata.com/myService2/" + getName() + "/"
                        + UUID.randomUUID());

        final RemoteServiceOptions options = new RemoteServiceOptions();

        final RemoteServiceFactoryImpl serviceFactory = new RemoteServiceFactoryImpl(
                options);

        try {

            // Verify not registered.
            assertNull(ServiceRegistry.getInstance().get(serviceURI1));
            assertNull(ServiceRegistry.getInstance().get(serviceURI2));

            // Register.
            ServiceRegistry.getInstance().add(serviceURI1, serviceFactory);

            // Verify discovery.
            assertNotNull(ServiceRegistry.getInstance().get(serviceURI1));
            assertNull(ServiceRegistry.getInstance().get(serviceURI2));

            // Verify same object.
            assertTrue(serviceFactory == ServiceRegistry.getInstance().get(
                    serviceURI1));

            /*
             * Can not register the serviceURI as an alias (for any registered
             * service URI).
             */
            try {
                ServiceRegistry.getInstance()
                        .addAlias(serviceURI1, serviceURI1);
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            /*
             * Now alias the service to the 2nd URI.
             */
            
            // Register alias.
            ServiceRegistry.getInstance().addAlias(serviceURI1, serviceURI2);

            // Verify discovery.
            assertNotNull(ServiceRegistry.getInstance().get(serviceURI1));
            assertNotNull(ServiceRegistry.getInstance().get(serviceURI2));

            // Verify same object.
            assertTrue(serviceFactory == ServiceRegistry.getInstance().get(
                    serviceURI1));
            assertTrue(serviceFactory == ServiceRegistry.getInstance().get(
                    serviceURI2));

            /*
             * Can not re-register the same alias.
             */
            try {
                ServiceRegistry.getInstance()
                        .addAlias(serviceURI1, serviceURI2);
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

        } finally {

            // De-register.
            ServiceRegistry.getInstance().remove(serviceURI1);
            ServiceRegistry.getInstance().remove(serviceURI2);

        }

        // Verify not registered.
        assertNull(ServiceRegistry.getInstance().get(serviceURI1));
        assertNull(ServiceRegistry.getInstance().get(serviceURI2));

        /*
         * Verify that we can re-register the alias.
         */
        try {
            // Register alias.
            ServiceRegistry.getInstance().addAlias(serviceURI1, serviceURI2);
        } finally {
            // Remove alias.
            ServiceRegistry.getInstance().remove(serviceURI2);
        }

    }

    /**
     * Unit test verifies that you MAY register an alias for a URI which is NOT
     * associated with an explicitly registered service.
     */
    public void test_serviceAlias2() {

        final AbstractTripleStore store = getStore(getProperties());

        try {

            /*
             * Declare terms that we will need.
             */
            
            final BigdataValueFactory f = store.getValueFactory();

            final BigdataURI serviceURI1 = f
                    .createURI("http://www.bigdata.com/myService/" + getName()
                            + "/" + UUID.randomUUID());

            final BigdataURI serviceURI2 = f
                    .createURI("http://www.bigdata.com/myService2/" + getName()
                            + "/" + UUID.randomUUID());

            final BigdataValue[] values = new BigdataValue[] { //
                    serviceURI1,//
                    serviceURI2,//
            };

            store.getLexiconRelation()
                    .addTerms(values, values.length, false/* readOnly */);

            try {

                // Verify not registered.
                assertNull(ServiceRegistry.getInstance().get(serviceURI1));
                assertNull(ServiceRegistry.getInstance().get(serviceURI2));

                // Register alias.
                ServiceRegistry.getInstance()
                        .addAlias(serviceURI1, serviceURI2);

                // Verify discovery (both null since neither paired with Service).
                assertNull(ServiceRegistry.getInstance().get(serviceURI1));
                assertNull(ServiceRegistry.getInstance().get(serviceURI2));

                /*
                 * Verify create for both end points. One is an alias. Neither
                 * is explicitly paired with a ServiceFactory.
                 */
                final QueryEngine queryEngine = QueryEngineFactory.getInstance()
                        .getQueryController(store.getIndexManager());

//                final ClientConnectionManager cm = queryEngine
//                        .getClientConnectionManager();
                final HttpClient cm = queryEngine.getClientConnectionManager();

                final JoinGroupNode groupNode = new JoinGroupNode();
                
                final ServiceNode serviceNode = new ServiceNode(
                        new ConstantNode(serviceURI1.getIV()), groupNode);

                // the end point which was aliased.
                {
                    
                    final ServiceCall<?> serviceCall = ServiceRegistry
                            .getInstance().toServiceCall(store, cm,
                                    serviceURI1, serviceNode, null /* BOpStats */);
                    
                    assertNotNull(serviceCall);
                    
                }

                // the end point which we did not alias.
                {

                    final ServiceCall<?> serviceCall = ServiceRegistry
                            .getInstance().toServiceCall(store, cm,
                                    serviceURI2, serviceNode, null /* BOpStats */);
                    
                    assertNotNull(serviceCall);

                }

            } finally {

                // De-register alias
                ServiceRegistry.getInstance().remove(serviceURI1);

            }

        } finally {

            store.destroy();

        }

    }


    /**
     * Unit test a {@link CustomServiceFactory} which hooks the connection start
     * for the {@link BigdataSail}.
     * 
     * @throws SailException
     */
    public void test_customService() throws SailException {

        final URI serviceURI1 = new URIImpl("http://www.bigdata.com/myService/"
                + getName() + "/" + UUID.randomUUID());

        final MyCustomServiceFactory serviceFactory = new MyCustomServiceFactory(
                new OpenrdfNativeServiceOptions());

        try {

            // Verify not registered.
            assertNull(ServiceRegistry.getInstance().get(serviceURI1));

            // Register.
            ServiceRegistry.getInstance().add(serviceURI1, serviceFactory);

            // Verify discovery.
            assertNotNull(ServiceRegistry.getInstance().get(serviceURI1));

            // Verify same object.
            assertTrue(serviceFactory == ServiceRegistry.getInstance().get(
                    serviceURI1));

            /*
             * Verify custom service visitation.
             */
            {

                final Iterator<CustomServiceFactory> itr = ServiceRegistry
                        .getInstance().customServices();

                boolean found = false;

                while (itr.hasNext()) {

                    final CustomServiceFactory t = itr.next();

                    if (t == serviceFactory) {

                        found = true;

                    }

                }

                assertTrue(found);

            }

            /*
             * Verify hooked on connection start.
             */
            {
                final AbstractTripleStore store = getStore(getProperties());
                try {

                    final BigdataSail sail = new BigdataSail(store.getNamespace(),store.getIndexManager());
                    try {
                        sail.initialize();
                        // Nothing started yet.
                        assertEquals("nstarted", 0,
                                serviceFactory.nstarted.get());
                        final BigdataSailConnection conn = sail.getConnection();
                        try {
                            // Verify the service was notified.
                            assertEquals("nstarted", 1,
                                    serviceFactory.nstarted.get());
                        } finally {
                            conn.close();
                        }

                    } finally {
                        sail.shutDown();
                    }

                } finally {
                    store.destroy();
                }
            }

            // Verify the service was notified just once.
            assertEquals("nstarted", 1, serviceFactory.nstarted.get());

        } finally {

            // De-register.
            ServiceRegistry.getInstance().remove(serviceURI1);

        }

        // Verify not registered.
        assertNull(ServiceRegistry.getInstance().get(serviceURI1));

    }

    /**
     * Private helper class used to verify that new mutable connections are
     * hooked.
     */
    private static class MyCustomServiceFactory extends CustomServiceFactoryBase {

        public final AtomicInteger nstarted = new AtomicInteger();

        private final IServiceOptions serviceOptions;

        public MyCustomServiceFactory(final IServiceOptions serviceOptions) {

            this.serviceOptions = serviceOptions;

        }

        @Override
        public IServiceOptions getServiceOptions() {
            return serviceOptions;
        }

        @Override
        public ServiceCall<?> create(ServiceCallCreateParams params) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startConnection(BigdataSailConnection conn) {

            nstarted.incrementAndGet();

        }
           
    }

    
}
