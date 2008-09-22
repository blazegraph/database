package com.bigdata.service.jini;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.LookupLocatorDiscovery;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A helper class that starts all the necessary services for a Jini federation.
 * This is used when testing, but NOT for benchmarking performance. For
 * benchmarking you MUST connect to an existing federation, ideally one deployed
 * over a cluster of machines!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniServicesHelper {

    public MetadataServer metadataServer0;

    public DataServer dataServer1;

    public DataServer dataServer0;

    public LoadBalancerServer loadBalancerServer0;

    public TimestampServer timestampServer0;

    public ResourceLockServer resourceLockServer0;

    public JiniClient client;

    /**
     * Looks for configuration files in the directory identified by the path
     * and starts the various services required by a {@link JiniFederation}.
     * This class assumes that the following configuration files will exist
     * in the directory identified by <i>path</i>.
     * 
     * <ul>
     * <li>ResourceLockServer0.config</li>
     * <li>MetadataServer0.config</li>
     * <li>DataServer0.config</li>
     * <li>DataServer1.config</li>
     * <li>LoadBalancerServer0.config</li>
     * <li>TimestampServer0.config</li>
     * <li>Client.config</li>
     * </ul>
     * 
     * @param path
     *            The path to the configuration files.
     */
    public JiniServicesHelper(String path) {

        this.path = path;

    }

    private final String path;

    private ExecutorService threadPool = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
    
    /**
     * Starts all services and connects the {@link JiniClient} to the
     * federation.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     */
    public void start() {

        // @todo verify that this belongs here vs in a main(String[]).
        System.setSecurityManager(new SecurityManager());

        /*
         * Start up a resource lock server.
         */
        threadPool.execute(resourceLockServer0 = new ResourceLockServer(
                new String[] { path + "ResourceLockServer0.config" }));


        /*
         * Start up a timestamp server.
         */
        threadPool.execute(timestampServer0 = new TimestampServer(
                new String[] { path + "TimestampServer0.config" }));

        
        /*
         * Start up a data server before the metadata server so that we can
         * make sure that it is detected by the metadata server once it
         * starts up.
         */
        threadPool.execute(dataServer1 = new DataServer(
                new String[] { path + "DataServer1.config" }));

        /*
         * Start up a load balancer server.
         */
        threadPool.execute(loadBalancerServer0 = new LoadBalancerServer(
                new String[] { path + "LoadBalancerServer0.config" }));

        /*
         * Start the metadata server.
         */
        threadPool.execute(metadataServer0 = new MetadataServer(
                new String[] { path + "MetadataServer0.config" }));

        /*
         * Start up a data server after the metadata server so that we can
         * make sure that it is detected by the metadata server once it
         * starts up.
         */
        threadPool.execute(dataServer0 = new DataServer(new String[] { path
                + "DataServer0.config" }));

        client = JiniClient
                .newInstance(new String[] { path + "Client.config" });

        // connect the client - this will get discovery running.
        client.connect();

        // Wait until all the services are up.
        getServiceID(resourceLockServer0);
        getServiceID(timestampServer0);
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(dataServer1);
        getServiceID(loadBalancerServer0);

        //          // wait/verify that the client has/can get the various services.
        //          for (int i = 0; i < 3; i++) {
        //
        //                int nwaiting = 0;
        //
        //                if (fed.getResourceLockService() == null) {
        //                    log.warn("No resource lock service yet...");
        //                    nwaiting++;
        //                }
        //
        //                if (fed.getMetadataService() == null) {
        //                    log.warn("No metadata service yet...");
        //                    nwaiting++;
        //                }
        //
        //                if (fed.getTimestampService() == null) {
        //                    log.warn("No timestamp service yet...");
        //                    nwaiting++;
        //                }
        //
        //                if (fed.getLoadBalancerService() == null) {
        //                    log.warn("No load balancer service yet...");
        //                    nwaiting++;
        //                }
        //
        //                if (nwaiting > 0) {
        //
        //                    log.warn("Waiting for " + nwaiting + " services");
        //
        //                    Thread.sleep(1000/* ms */);
        //
        //                }
        //
        //            }
        //
        //          assertNotNull("No lock service?", fed.getResourceLockService());
        //
        //          assertNotNull("No timestamp service?", fed.getTimestampService());
        //
        //          assertNotNull("No metadata service?", fed.getMetadataService());
        //
        //          assertNotNull("No load balancer service?", fed.getLoadBalancerService());

    }

    /**
     * Immediate shutdown.
     */
    public void shutdown() {

        if (client != null && client.isConnected()) {

            client.disconnect(true/* immediateShutdown */);

            client = null;

        }
        
        threadPool.shutdownNow();
        
//        if (metadataServer0 != null) {
//
//            metadataServer0.shutdownNow();
//
//            metadataServer0 = null;
//
//        }
//
//        if (dataServer0 != null) {
//
//            dataServer0.shutdownNow();
//
//            dataServer0 = null;
//
//        }
//
//        if (dataServer1 != null) {
//
//            dataServer1.shutdownNow();
//
//            dataServer1 = null;
//
//        }
//
//        if (loadBalancerServer0 != null) {
//
//            loadBalancerServer0.shutdownNow();
//
//            loadBalancerServer0 = null;
//
//        }
//
//        if (timestampServer0 != null) {
//
//            timestampServer0.shutdownNow();
//
//            timestampServer0 = null;
//
//        }
//
//        if (resourceLockServer0 != null) {
//
//            resourceLockServer0.shutdownNow();
//
//            resourceLockServer0 = null;
//
//        }

    }

    /**
     * Shuts down and <em>destroys</em> the services in the federation. The
     * shutdown is abrubt. You can expect to see messages about interrupted IO
     * such as
     * 
     * <pre>
     * java.rmi.MarshalException: error marshalling arguments; nested exception is: 
     *     java.io.IOException: request I/O interrupted
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:785)
     *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
     *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
     *     at $Proxy5.notify(Ljava.lang.String;Ljava.util.UUID;Ljava.lang.String;[B)V(Unknown Source)
     * </pre>
     * 
     * These messages can be safely ignored IF they occur during this method.
     */
    public void destroy() {

        if (client != null && client.isConnected()) {

            client.disconnect(true/* immediateShutdown */);

            client = null;

        }

        if (metadataServer0 != null) {

            metadataServer0.destroy();

            metadataServer0 = null;

        }

        if (dataServer0 != null) {

            dataServer0.destroy();

            dataServer0 = null;

        }

        if (dataServer1 != null) {

            dataServer1.destroy();

            dataServer1 = null;

        }

        if (loadBalancerServer0 != null) {

            loadBalancerServer0.destroy();

            loadBalancerServer0 = null;

        }

        if (timestampServer0 != null) {

            timestampServer0.destroy();

            timestampServer0 = null;

        }

        if (resourceLockServer0 != null) {

            resourceLockServer0.destroy();

            resourceLockServer0 = null;

        }

    }

    /**
     * Return the {@link ServiceID} of a server that we started ourselves.
     * The method waits until the {@link ServiceID} becomes available on
     * {@link AbstractServer#getServiceID()}.
     * 
     * @throws RuntimeException
     *                If the {@link ServiceID} can not be found after a
     *                timeout.
     * 
     * @throws RuntimeException
     *                if the thread is interrupted while it is waiting to
     *                retry.
     */
    static private ServiceID getServiceID(AbstractServer server) {

        ServiceID serviceID = null;

        for (int i = 0; i < 20 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchonously by the registrar.
             */

            serviceID = server.getServiceID();

            if (serviceID == null) {

                /*
                 * We wait a bit and retry until we have it or timeout.
                 */

                try {

                    Thread.sleep(200);

                } catch (InterruptedException e) {

                    throw new RuntimeException("Interrupted: " + e, e);

                }

            }

        }

        if (serviceID == null)
            throw new AssertionError();

        return serviceID;

    }

    /**
     * Return <code>true</code> if Jini appears to be running on the
     * localhost.
     * 
     * @throws Exception
     */
    public static boolean isJiniRunning() {
        
        return isJiniRunning(new String[] { "jini://localhost/" });
        
    }
    
    /**
     * @param url
     *            One or more unicast URIs of the form <code>jini://host/</code>
     *            or <code>jini://host:port/</code> -or- an empty array if you
     *            want to use <em>multicast</em> discovery.
     */
    public static boolean isJiniRunning(String[] url) {
        
        final LookupLocator[] locators = new LookupLocator[url.length];

        for (int i = 0; i < url.length; i++) {
           
            try {

                locators[i] = new LookupLocator("jini://localhost/");

            } catch (MalformedURLException e) {

                throw new RuntimeException(e);

            }
            
        }

        final LookupLocatorDiscovery discovery = new LookupLocatorDiscovery(
                locators);

        try {

            final long timeout = 2000; // ms

            final long begin = System.currentTimeMillis();

            long elapsed;

            while ((elapsed = (System.currentTimeMillis() - begin)) < timeout) {

                ServiceRegistrar[] registrars = discovery.getRegistrars();

                if (registrars.length > 0) {

                    System.err.println("Found " + registrars.length
                            + " registrars in " + elapsed + "ms.");

                    return true;

                }

            }

            System.err
                    .println("Could not find any service registrars on localhost after "
                            + elapsed + " ms");

            return false;

        } finally {

            discovery.terminate();

        }

    }

}
