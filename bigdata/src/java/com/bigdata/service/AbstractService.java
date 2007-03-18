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
 * Created on Mar 18, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.io.Serializable;
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

import org.apache.log4j.Logger;

/**
 * Abstract base class for services discoverable using JINI.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractService implements DiscoveryListener, LeaseListener, IServiceShutdown
{

    public static Logger log = Logger.getLogger(AbstractService.class);

    private ServiceID serviceID; 
    private ServiceItem item;
    private ServiceRegistration reg;
    final private LeaseRenewalManager leaseManager = new LeaseRenewalManager();

    /**
     * Close down the service (no longer discoverable).
     * 
     * @todo revoke lease, etc.
     */
    private void _close() {
        
    }

    /**
     * Waits for existing leases to expire before shutting down, but does not
     * grant any new leases.
     */
    public void shutdown() {
        
        /*
         * @todo wait for leases to expire and then shutdown. do not renew any
         * leases.
         */
        
        _close();
        
    }
    
    /**
     * Shutdown immediately, violating any existing leases.
     */
    public void shutdownNow() {
        
        _close();
        
    }
    
    /**
     * Return the {@link ServiceID}. Using a consistent {@link ServiceID} makes
     * it easier to register the same service against multiple lookup services.
     * <p>
     * This implementation generates a new {@link ServiceID} each time.
     * 
     * @todo If you want to restart (or re-register) the same service, then you
     *       need to read the serviceID from some persistent location. If you
     *       are using activation, then the service can be remotely started
     *       using its serviceID which takes that responsibility out of your
     *       hands. When using activation, you will only create a serviceID once
     *       when you install the service onto some component and activition
     *       takes responsiblity for starting the service on demand.
     */
    public ServiceID getServiceID() {

        Uuid uuid = UuidFactory.generate();
        
        return new ServiceID(uuid.getMostSignificantBits(), uuid
                .getLeastSignificantBits());

    }

    /**
     * Server startup performs asynchronous multicast lookup discovery. The
     * {@link #discovered(DiscoveryEvent)} method is invoked asynchronously to
     * register {@link TestServerImpl} instances.
     */
    public AbstractService() {

        serviceID = getServiceID();

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

        AbstractService.log.info("Discovered "+registrars.length+" service registrars");

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
                        new ServiceInfo("bigdata", // product or package name
                                "SYSTAP,LLC", // manufacturer
                                "SYSTAP,LLC", // vendor
                                "0.1-beta", // version
                                "bigdata", // model
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
                        AbstractService.log.info("lease will expire in "
                                + (reg.getLease().getExpiration() - System
                                        .currentTimeMillis()) + "ms");
                        leaseManager
                                .renewUntil(reg.getLease(), Lease.FOREVER, AbstractService.this);
                        /*
                         * Service has been registered and lease renewal is
                         * operating.
                         */
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

        AbstractService.log.info("");
        
    }

    /**
     * Note: This is only invoked if the automatic lease renewal by the
     * lease manager is denied by the service registrar. In that case we
     * should probably update the Status to indicate an error condition.
     */
    public void notify(LeaseRenewalEvent event) {

        AbstractService.log.error("Lease could not be renewed: " + event);
        
    }
    
    /**
     * Launch the server in a separate thread.
     */
    public static void launchServer() {
        new Thread("launchServer") {
            public void run() {
                AbstractService.main(new String[] {});
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
        AbstractService.log.info("Will start test server.");
        AbstractService testServer = new AbstractService(){
            /* @todo this is an anonymous class - each service must provide its
             * own main() and registration information.
             */
        };
        AbstractService.log.info("Started test server.");
        try {
            Thread.sleep(lifespan);
        }
        catch( InterruptedException ex ) {
            AbstractService.log.warn(ex);
        }
        /*
         * @todo This forces a hard reference to remain for the test server.
         */
        AbstractService.log.info("Server will die: "+testServer);
    }
    
    /**
     * {@link Status} is abstract so a service needs to provide their own
     * concrete implementation.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class MyStatus extends Status {

        /**
         * 
         */
        private static final long serialVersionUID = 3431522046169284463L;
        
        /**
         * Deserialization constructor (required).
         */
        public MyStatus(){}
        
        public MyStatus(StatusType statusType) {

            /*
             * Note: This just sets the read/write public [severity] field on
             * the super class.
             */
            super(statusType);
            
        }
        
    }
    
    /**
     * {@link ServiceType} is abstract so a service basically needs to provide
     * their own concrete implementation. This class does not support icons
     * (always returns null for {@link ServiceType#getIcon(int)}. See
     * {@link java.beans.BeanInfo} for how to interpret and support the
     * getIcon() method.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     */
    public static class MyServiceType extends ServiceType
    {

        /**
         * 
         */
        private static final long serialVersionUID = -2088608425852657477L;
        
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

    /**
     * The interfaces implemented by the service should be made locally
     * available to the client so that it can execute methods on those
     * interfaces without using reflection.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
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
         * Note: Mark the {@link Logger} as transient since we do NOT need to
         * serialize its state.
         */
        public static final transient Logger log = Logger.getLogger(TestServerImpl.class);

        /**
         * 
         */
        private static final long serialVersionUID = -920558820563934297L;

        /**
         * De-serialization constructor (required).
         */
        public TestServerImpl() {
//            System.err.println("Created: "+this);
            log.info("Created: "+this);
        }

        public void invoke() {
//            System.err.println("invoked: "+this);
            log.info("invoked: "+this);
        }

    }

}
