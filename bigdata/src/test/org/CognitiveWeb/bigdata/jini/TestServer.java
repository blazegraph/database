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
 * Created on Jun 19, 2006
 */
package org.CognitiveWeb.bigdata.jini;

import java.io.IOException;
import java.rmi.RemoteException;

import net.jini.core.entry.Entry;
import net.jini.core.lease.Lease;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceRegistration;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscovery;
import net.jini.id.Uuid;
import net.jini.id.UuidFactory;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.entry.Address;
import net.jini.lookup.entry.Comment;
import net.jini.lookup.entry.Location;
import net.jini.lookup.entry.Name;
import net.jini.lookup.entry.ServiceInfo;
import net.jini.lookup.entry.ServiceType;
import net.jini.lookup.entry.Status;
import net.jini.lookup.entry.StatusType;

import org.CognitiveWeb.bigdata.jini.TestServiceDiscovery.TestServerImpl;

/**
 * Launches a server used by the test. The server is launched in a separate
 * thread that will die after a timeout, taking the server with it. The server
 * exposes some methods for testing, notably a method to test remote method
 * invocation and one to shutdown the server.
 * 
 * @todo Look into {@link java.rmi.RMISecurityManager}. Simply adding this to
 *       main() causes things to fail, but gives us the trace that was otherwise
 *       missing.
 * 
 * @todo Look into the manager classes for service joins, discovery, etc. The
 *       code in this class can probably be simplified drammatically.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class TestServer implements DiscoveryListener, LeaseListener
{
    
    private ServiceID serviceID; 
    private ServiceItem item;
    private ServiceRegistration reg;
    final private LeaseRenewalManager leaseManager = new LeaseRenewalManager();

    /**
     * Server startup performs asynchronous multicast lookup discovery. The
     * {@link #discovered(DiscoveryEvent)}method is invoked asynchronously
     * to register {@link TestServerImpl}instances.
     */
    public TestServer() {

        /*
         * Generate a ServiceID ourselves. This makes it easier to register
         * the same service against multiple lookup services.
         * 
         * @todo If you want to restart (or re-register) the same service,
         * then you need to read the serviceID from some persistent
         * location. If you are using activation, then the service can be
         * remotely started using its serviceID which takes that
         * responsibility out of your hands. When using activation, you will
         * only create a serviceID once when you install the service onto
         * some component and activity takes responsiblity for starting the
         * service on demand.
         */
        Uuid uuid = UuidFactory.generate();
        serviceID = new ServiceID(uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits());

        try {

            LookupDiscovery discover = new LookupDiscovery(
                    LookupDiscovery.ALL_GROUPS);
            
            discover.addDiscoveryListener(this);
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

    /**
     * Log a message and register the {@link TestServerImpl}. Events are
     * aggregated but we can still receive multiple events depending on the
     * latency between discovery of various service registrars. Since the
     * multicast protocol was used to discover service registrars, we can wind
     * up registering with more than one registrar.
     * 
     * @todo If we do not receive this message after some timeout then the
     *       server should log an error (in main()) and exit with a non-zero
     *       status code. This means that the service needs to expose an
     *       indicator of whether or not it has been registered.
     * 
     * @todo I can't figure out how to get the service to show up with its
     *       metadata in the service browser.
     * 
     * @todo Modify service proxy to use RMI.
     * 
     * @todo This should be a fast operation and should not make remote calls.
     *       Service registration therefore needs to happen in another thread so
     *       that the service registrar can continue about its business.
     */
    public void discovered(DiscoveryEvent evt) {

        /*
         * At this point we have discovered one or more lookup services.
         */

        registerService( evt.getRegistrars() );
        
    }

    /**
     * Registers a service proxy with reach identified registrar. The
     * registration happens in another thread to keep the latency down for the
     * {@link DiscoveryListener#discovered(net.jini.discovery.DiscoveryEvent)}
     * event.
     * 
     * @param registrars
     *            One or more service registrars.
     */

    private void registerService( final ServiceRegistrar[] registrars ) {

        TestServiceDiscovery.log.info("Discovered "+registrars.length+" service registrars");

        new Thread("Register Service") {
            public void run() {
                // Create an information item about a service
                item = new ServiceItem(serviceID, new TestServerImpl(), new Entry[] {
                        new Comment("Test service(comment)"), // human facing comment.
                        new Name("Test service(name)"), // human facing name.
                        new MyServiceType("Test service (display name)",
                                "Test Service (short description)"), // service type
                        new MyStatus(StatusType.NORMAL),
                        new Location("floor","room","building"),
                        new Address("street", "organization", "organizationalUnit",
                                "locality", "stateOrProvince", "postalCode",
                                "country"), 
                        new ServiceInfo("BigTable", // product or package name
                                "SYSTAP,LLC", // manufacturer
                                "CognitiveWeb", // vendor
                                "0.1-beta", // version
                                "model", // model
                                "serial#" // serialNumber
                        ) });

                for (int i = 0; i < registrars.length; i++) {
                    /*
                     * Register a service. The service requests a "long" lease using
                     * Lease.FOREVER. The registrar will decide on the actual length
                     * of the lease.
                     */
                    ServiceRegistrar registrar = registrars[i];
                    while (reg == null) {
                        try {
                            // Register the service.
                            reg = registrar.register(item, Lease.FOREVER);
                        } catch (RemoteException ex) {
                            // retry until successful.
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException ex2) {
                            }
                        }
                        /*
                         * Setup automatic lease renewal.
                         * 
                         * Note: A single lease manager can handle multiple services
                         * and the lease of a given service on multiple service
                         * registrars. It knows which lease is about to expire and
                         * preemptively extends the lease on the behalf of the
                         * registered service. It will notify the LeaseListener iff
                         * it is unable to renew a lease.
                         */
                        TestServiceDiscovery.log.info("lease will expire in "
                                + (reg.getLease().getExpiration() - System
                                        .currentTimeMillis()) + "ms");
                        leaseManager
                                .renewUntil(reg.getLease(), Lease.FOREVER, TestServer.this);
                        // Service has been registered and lease renewal is
                        // operating.
                        break;
                    }
                }
           }
        }.start()
        ;
        
            
    }
    
    /**
     * Log a message.
     */
    public void discarded(DiscoveryEvent arg0) {

        TestServiceDiscovery.log.info("");
        
    }

    /**
     * Note: This is only invoked if the automatic lease renewal by the
     * lease manager is denied by the service registrar. In that case we
     * should probably update the Status to indicate an error condition.
     */
    public void notify(LeaseRenewalEvent event) {

        TestServiceDiscovery.log.error("Lease could not be renewed: " + event);
        
    }
    
    /**
     * Launch the server in a separate thread.
     */
    public static void launchServer() {
        new Thread("launchServer") {
            public void run() {
                TestServer.main(new String[] {});
            }
        }.start();
    }

    /**
     * Run the server. It will die after a timeout.
     * 
     * @param args
     *            Ignored.
     */
    public static void main(String[] args) {
        final long lifespan = 3 * 60 * 1000; // life span in seconds.
        TestServiceDiscovery.log.info("Will start test server.");
        TestServer testServer = new TestServer();
        TestServiceDiscovery.log.info("Started test server.");
        try {
            Thread.sleep(lifespan);
        }
        catch( InterruptedException ex ) {
            TestServiceDiscovery.log.warn(ex);
        }
        /*
         * @todo This forces a hard reference to remain for the test server.
         */
        TestServiceDiscovery.log.info("Server will die: "+testServer);
    }
    
    /**
     * {@link Status} is abstract so a service basically needs to provide their own
     * concrete implementation.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class MyStatus extends Status {

        /*
         * Note: public fields are required and must be Serializable.
         */
        public StatusType statusType;
    
        /**
         * Deserialization constructor (required).
         */
        public MyStatus(){}
        
        public MyStatus(StatusType statusType) {
            this.statusType = statusType;
        }
        
    }
    
    /**
     * {@link ServiceType}is abstract so a service basically needs to
     * provide their own concrete implementation. This class does not
     * support icons (always returns null for
     * {@link ServiceType#getIcon(int)}.  See {@link java.beans.BeanInfo}
     * for how to interpret and support the getIcon() method.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson </a>
     */
    public static class MyServiceType extends ServiceType
    {

        public String displayName;
        public String shortDescription;
        
        /**
         * Deserialization constructor (required).
         */
        public MyServiceType() {}

        public MyServiceType(String displayName, String shortDescription) {
            this.displayName = displayName;
            this.shortDescription = shortDescription;
        }
        
        public String getDisplayName() {
            return displayName;
        }
        
        public String getShortDescription() {
            return shortDescription;
        }
        
    }

}