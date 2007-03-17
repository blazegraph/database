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
package org.CognitiveWeb.bigdata.jini;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;

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
 * from the codebase URL. I jump start the tests myself using
 * </p>
 * 
 * <pre>
 *   -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/
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
 * @todo Explore the jini {@link net.jini.core.transaction.Transaction}model.
 *       Perhaps we can use this as is to support two phase commits across the
 *       database segments? The transaction model does not impose any semantics,
 *       e.g., there is no locking, but we can handle all of that.
 * 
 * @see http://archives.java.sun.com/cgi-bin/wa?A2=ind0311&L=jini-users&F=&S=&P=7182
 *      for a description of policy files and
 *      http://www.dancres.org/cottage/jini-start-examples-2_1.zip for the
 *      policy files described.<br>
 *      When testing standalone with only trusted code and NO downloaded code,
 *      it is reasonable to consider running the test code using
 *      "-Djava.security.policy=policy.all" so that you can get things moving.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public class TestServiceDiscovery extends TestCase {

    public static Logger log = Logger.getLogger(TestServiceDiscovery.class);
    
    /**
     * Tests spans several services and verifies that we can discovery each of
     * them.
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    
    public void test_serviceDiscovery() throws IOException, ClassNotFoundException {

	/*
     * install suitable security manager. this is required before the
     * application can download code.
     */
	System.setSecurityManager(new SecurityManager());

	/*
         * Launch the server to which we will connect.
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
         * What follows is an example of service lookup, but we have to register
         * the service first.
         */
        
        // Prepare a template for lookup search
	ServiceTemplate template = new ServiceTemplate(null,
                new Class[] { TestServerImpl.class }, null);

        /*
         * Lookup a service. This can fail if the service registrar has not
         * finished processing the service registration. If it does, you can
         * generally just retry the test and it will succeed. However this
         * points out that the client may need to wait and retry a few times if
         * you are starting everthing up at once (or just register for
         * notification events for the service if it is not found and enter a
         * wait state).
         */
	TestServerImpl service = null;
	for( int i=0; i<10 && service == null; i++) {
	    service = (TestServerImpl) serviceRegistrar.lookup(template /*, maxMatches*/);
	    if( service == null ) {
	        log.info("Service not found: sleeping...");
	        try {Thread.sleep(100);}
	        catch( InterruptedException ex) {}
	    }
	}

	assertNotNull("Could not find service.", service );
	
	service.invoke();
	
	// Sleep for a bit so that I can inspect the service in the Service Browser.
        try {Thread.sleep(10000);}
        catch( InterruptedException ex) {}
	
    }
    
    public static interface ITestService
    {

        /**
         * Method for testing remote invocation.
         */
        public void invoke();
                
    }

    /**
     * The proxy object that gets passed around.
     * 
     * @todo It appears that multiple instances of this class are getting
     *       created. This is consistent with the notion that the instance is
     *       being "passed" around by state and not by reference. This implies
     *       that instances are not consumed when they are discovered but merely
     *       cloned using Java serialization.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     */
    public static class TestServerImpl implements ITestService, Serializable
    {

        /**
         * 
         */
        private static final long serialVersionUID = -920558820563934297L;

        /**
         * De-serialization constructor (required).
         */
        public TestServerImpl() {
            log.info("Created: "+this);
        }

        public void invoke() {
            log.info("invoked: "+this);
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
