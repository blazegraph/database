/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.jini.start.ServicesManagerServer;
import com.bigdata.journal.ITx;
import com.bigdata.service.jini.util.JiniServicesHelper;

/**
 * Proxy test suite for {@link ScaleOutTripleStore} running against an
 * distributed federation using Jini services.
 * <p>
 * Note: The tests in this suite setup a bigdata federation for each test. The
 * various data services for the federation all run in the same process, but the
 * data services register themselves and are discovered using jini and
 * communications with those data services use remote procedure calls (at least
 * logically since some JVMs may optimize away the RPC to a local endpoint).
 * <p>
 * Jini MUST be running.
 * <p>
 * You MUST specify at least the following properties to the JVM and have access
 * to the resources in <code>src/resources/config</code>.
 * 
 * <pre>
 *         -Djava.security.policy=policy.all
 * </pre>
 * 
 * Note: You will sometimes see a connection refused exception thrown while
 * trying to register the scale-out indices. This happens when some data
 * services were abruptly terminated and those services have not yet been
 * de-registered from jini. A little wait or a restart of Jini fixes this
 * problem.
 * <p>
 * Note: All configuration of the services and the client are performed using
 * the files in <code>src/resources/config/standalone</code>. The
 * <code>.config</code> files have the jini aspect of the client and services
 * configurations. The <code>.property</code> files have the bigdata aspect of
 * the client and services configurations.
 * <p>
 * Note: Normally the services are destroyed after each test, which has the
 * effect of removing all files create by each service. Therefore tests normally
 * start from a clean slate. If you have test setup problems, such as "metadata
 * index exists", first verify that the <code>standalone</code> directory in
 * which the federation is created has been properly cleaned. If files have been
 * left over due to an improper cleanup by a prior test run then you can see
 * such startup problems.
 * 
 * @todo consider reusing a single federation for all unit tests in order to
 *       reduce the setup time for the tests. this would have the consequence
 *       that we are not guarenteed a clean slate when we connect to the
 *       federation. we would need to use dropIndex.
 * 
 * @todo the jini service browser log contains exceptions - presumably because
 *       we have not setup a codebase, e.g., using an embedded class server,
 *       such that it is unable to resolve the various bigdata classes.
 * 
 * @todo could the {@link ServicesManagerServer}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestScaleOutTripleStoreWithJiniFederation extends AbstractTestCase {

    public TestScaleOutTripleStoreWithJiniFederation() {

    }

    public TestScaleOutTripleStoreWithJiniFederation(String name) {

        super(name);
        
    }
    
    public static Test suite() {

        final TestScaleOutTripleStoreWithJiniFederation delegate = new TestScaleOutTripleStoreWithJiniFederation(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Scale-Out Triple Store Test Suite (jini federation)");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
//        // writes on the term:id and id:term indices.
//        suite.addTestSuite(TestTermAndIdsIndex.class);
//
//        // writes on the statement indices.
//        suite.addTestSuite(TestStatementIndex.class);
               
        /*
         * Proxied test suite for use only with the LocalTripleStore.
         * 
         * @todo test unisolated operation semantics.
         */

//        suite.addTestSuite(TestFullTextIndex.class);

//        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        // basic test suite.
        suite.addTest(TestTripleStoreBasics.suite());
        
        // rules, inference, and truth maintenance test suite.
        suite.addTest(com.bigdata.rdf.rules.TestAll.suite());

        return suite;

    }
    
    private JiniServicesHelper helper;
    
    public void setUp() throws Exception {
        
        super.setUp();

        System.setSecurityManager(new SecurityManager());
        
    }

    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp(final ProxyTestCase testCase) throws Exception {

        super.setUp(testCase);
        
        helper = new JiniServicesHelper();
        
        helper.start();
        
    }
    
    public void tearDown(final ProxyTestCase testCase) throws Exception {

        if (helper != null) {

            helper.destroy();

            // Note: must clear reference or junit will hold onto it.
            helper = null;
            
        }
        
        super.tearDown();
        
    }
    
    private AtomicInteger inc = new AtomicInteger();

    protected AbstractTripleStore getStore(final Properties properties) {
        
        // Note: distinct namespace for each triple store created on the federation.
        final String namespace = "test"+inc.incrementAndGet();
   
        // Connect to the triple store.
        final AbstractTripleStore store = new ScaleOutTripleStore(helper.client
                .getFederation(), namespace, ITx.UNISOLATED, properties);

        store.create();
        
        return store;
        
    }
 
    /**
     * Re-open the same backing store.
     * <p>
     * This basically disconnects the client
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed, or if the store can not
     *                be re-opened, e.g., from failure to obtain a file lock,
     *                etc.
     */
    protected AbstractTripleStore reopenStore(final AbstractTripleStore store) {

        final String namespace = store.getNamespace();

        store.close();

        // close the client connection to the federation.
        helper.client.disconnect(true/* immediateShutdown */);

        // re-connect to the federation.
        helper.client.connect();

        // obtain view of the triple store.
        return new ScaleOutTripleStore(helper.client.getFederation(),
                namespace, ITx.UNISOLATED, store.getProperties());

    }

}
