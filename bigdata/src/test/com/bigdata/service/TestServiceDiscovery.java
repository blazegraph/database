/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jun 16, 2006
 */
package com.bigdata.service;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.ConnectException;

import junit.framework.TestCase;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;

import org.apache.log4j.Logger;

/**
 * <p>
 * Test the ability to register, discover, and invoke a jini service.
 * </p>
 * <p>
 * Note: jini MUST be running. You can get the jini starter kit and install it
 * to get jini running.
 * </p>
 * <p>
 * Note: The registered service will NOT show up correctly in the Service
 * Browser (you will see "Unknown service") unless you set the codebase when
 * executing this test class and the .class files are available for download
 * from the codebase URL. I jump start the tests myself by copying them onto an
 * HTTP server and using the following options in the command line to execute
 * the test:
 * </p>
 * 
 * <pre>
 *    -Djava.security.policy=policy.all
 *    -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/
 * </pre>
 * 
 * <p>
 * which presuposes that the required class files are on that server available
 * for download. The security policy is overlax, but you do need to grant some
 * privledges in order to partitipate in discovery, etc.
 * </p>
 * 
 * @todo While service lookup is unicast, service registration is multicast.
 *       Probably both should be unicast for the purposes of this test.
 * 
 * @todo The basic configuration should require only jini-core.jar. Figure out
 *       how to get remote class loading working so that the requirements on a
 *       client remain that minimal. Right now I am also using jini-ext.jar,
 *       reggie.jar and sun-util.jar to run this test. jini-ext.jar is the big
 *       one at over 1M. (this can be facilitated using the dljar ant task and
 *       specifing jini-core as the target platform.)
 * 
 * @todo Figure out how to divide the service into a proxy and a remote object.
 *       We need this in order to measure the cost of sending data across the
 *       network.
 * 
 * @todo Explore the jini {@link net.jini.core.transaction.Transaction} model.
 *       Perhaps we can use this as is to support two phase commits across the
 *       database segments? The transaction model does not impose any semantics,
 *       e.g., there is no locking, but we can handle all of that.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class TestServiceDiscovery extends TestCase {

    public static Logger log = Logger.getLogger(TestServiceDiscovery.class);
    
    /**
     * Tests launches a service and verifies that we can discovery it and invoke
     * an operation exposed by that service.
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void test_serviceDiscovery() throws IOException,
            ClassNotFoundException {

        /*
         * Install suitable security manager. this is required before the client
         * can download code. The code will be downloaded from the HTTP server
         * identified by the codebase property specified for the VM running the
         * service. For the purposes of this test, we are using the _same_ VM to
         * run the client and the service, so you have to specify the codebase
         * property when running the test.
         */
        System.setSecurityManager(new SecurityManager());

        /*
         * Launch the server to which we will connect (this is being done by the
         * test harness rather than setting up activation for the service since
         * we want to explicitly control the service instances for the purpose
         * of testing).
         */
        TestServer.launchServer();
        
        /* 
         * Lookup the discover service (unicast on localhost).
         */

        // get the hostname.
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        // Find the service registrar (unicast protocol).
        final int timeout = 4*1000; // seconds.
        System.err.println("hostname: "+hostname);
        LookupLocator lookupLocator = new LookupLocator("jini://"+hostname);
        ServiceRegistrar serviceRegistrar = lookupLocator.getRegistrar( timeout );

        /*
         * Prepare a template for lookup search.
         * 
         * Note: The client needs a local copy of the interface in order to be
         * able to invoke methods on the service without using reflection. The
         * implementation class will be downloaded from the codebase identified
         * by the server.
         */
        ServiceTemplate template = new ServiceTemplate(//
                /*
                 * use this to request the service by its serviceID.
                 */
                null,
                /*
                 * Use this to filter services by an interface that they expose.
                 */
                new Class[] { ITestService.class },
                /*
                 * use this to filter for services by Entry attributes.
                 */
                null);

        /*
         * Lookup a service. This can fail if the service registrar has not
         * finished processing the service registration. If it does, you can
         * generally just retry the test and it will succeed. However this
         * points out that the client may need to wait and retry a few times if
         * you are starting everthing up at once (or just register for
         * notification events for the service if it is not found and enter a
         * wait state).
         */
        ITestService service = null;
        for (int i = 0; i < 10 && service == null; i++) {
            service = (ITestService) serviceRegistrar
                    .lookup(template /* , maxMatches */);
            if (service == null) {
                log.info("Service not found: sleeping...");
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ex) {
                }
            }
        }

        assertNotNull("Could not find service.", service);

        service.invoke();

        // Sleep for a bit so that I can inspect the service in the Service
        // Browser.
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
        }

        /*
         * try service invocation again. if the service lease is canceled before
         * we do this then we should see an exception here.
         */
        try {
            service.invoke();
            fail("Expecting "+ConnectException.class);
        } catch(ConnectException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

    }
    
    public static void main(String[] args) throws Exception {
        
        TestServiceDiscovery test = new TestServiceDiscovery();
        
        test.setUp();

        try {
        
            test.test_serviceDiscovery();
        
        } finally {
        
            test.tearDown();

        }
        
    }
    
}
