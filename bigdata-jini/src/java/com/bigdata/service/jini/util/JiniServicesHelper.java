package com.bigdata.service.jini.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.ZookeeperClientConfig;
import com.bigdata.jini.start.config.ZookeeperServerConfiguration;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.jini.util.ConfigMath;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.resources.ResourceFileFilter;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.AbstractServer;
import com.bigdata.service.jini.ClientServer;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.FakeLifeCycle;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.LoadBalancerServer;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.TransactionServer;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.zookeeper.ZooHelper;

/**
 * A helper class that starts all the necessary services for a Jini federation.
 * This is used when testing, but NOT for benchmarking performance. For
 * benchmarking you MUST connect to an existing federation, ideally one deployed
 * over a cluster of machines!
 * <p>
 * Note: You MUST specify a sufficiently lax security policy, for example:
 * 
 * <pre>
 * -Djava.security.policy=policy.all
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniServicesHelper extends JiniCoreServicesHelper {

    protected final static Logger log = Logger
            .getLogger(JiniServicesHelper.class);

    public MetadataServer metadataServer0;

    public DataServer dataServer1;

    public DataServer dataServer0;

    public LoadBalancerServer loadBalancerServer0;

    public TransactionServer transactionServer0;

    public ClientServer clientServer0;

    public JiniClient<?> client;

//    public ZooKeeper zookeeper;
//    
//    public ZookeeperClientConfig zooConfig;

    public JiniFederation<?> getFederation() {
        
        return client.getFederation();
        
    }
    
    /**
     * Return a proxy for #dataServer0.
     */
    public IDataService getDataService0(){

        return client.getFederation().getDataService(
                JiniUtil.serviceID2UUID(dataServer0.getServiceID()));

    }
    
    /**
     * Return a proxy for #dataServer1.
     */
    public IDataService getDataService1(){

        return client.getFederation().getDataService(
                JiniUtil.serviceID2UUID(dataServer1.getServiceID()));

    }

    /**
     * The default configuration file for stand alone testing.
     */
    public static final File CONFIG_STANDALONE = new File(
            "bigdata-jini/src/resources/config/bigdataStandaloneTesting.config");

    /**
     * Return a new file whose contents are the byte-by-byte concatenation of
     * the {@link #CONFIG_STANDALONE} configuration file and a configuration
     * file specified by the caller, so leave some whitespace at the top or
     * bottom of one of the files. The new file is created using the temporary
     * file mechanism and SHOULD be deleted by the caller when they are done
     * with it. This is used to write unit tests which depend on the
     * configuration of components not specified in {@link #CONFIG_STANDALONE}.
     * 
     * @param file
     *            The additional configuration file. This file should only
     *            include the component configuration without any imports.
     * 
     * @return The new configuration file.
     * 
     * @throws IOException
     */
    public static File append(final File file) throws IOException {

        if (file == null)
            throw new IllegalArgumentException();

        if (!file.isFile())
            throw new FileNotFoundException(file.getPath());

        final File tmp = File.createTempFile("bigdata-", ".config");

        InputStream is = null;
        OutputStream os = null;
        try {

            is = new BufferedInputStream(new FileInputStream(CONFIG_STANDALONE));
            os = new BufferedOutputStream(new FileOutputStream(tmp));

            int b;
            while ((b = is.read()) != -1) {

                os.write(b);
                
            }
            
            is.close();

            is = null;
            
            is = new BufferedInputStream(new FileInputStream(file));

            while ((b = is.read()) != -1) {

                os.write(b);
                
            }
            
            is.close();
            
            return tmp;

        } catch(Throwable t) {
            
            tmp.delete();
            
            throw new RuntimeException(t);
            
        } finally {

            if (is != null) {
                is.close();
            }

            if (os != null) {
                os.close();
            }

        }
        
    }
    
    /**
     * New helper instance using {@link #CONFIG_STANDALONE}. Use
     * {@link #start()} and {@link #destroy()} to start and await the services
     * and to tear them down respectively.
     */
    public JiniServicesHelper() {

        this(new String[] { CONFIG_STANDALONE.getPath() });
        
    }

    /**
     * New helper instance. Use {@link #start()} and {@link #destroy()} to start
     * and await the services and to tear them down respectively.
     * <p>
     * Note: Constructing a different configuration file for testing purposes or
     * applying overrides must be done very carefully. It is generally safe to
     * override certain parameters for the various services, either in a copy of
     * the {@link #CONFIG_STANDALONE} file or in jini style overrides. However,
     * in order to start more than one instance of a service you must modify
     * {@link #start()} to explicitly override the
     * {@link AbstractServer.ConfigurationOptions#SERVICE_DIR} for each
     * instance.
     * 
     * @param args
     *            The arguments specify the configuration file (required) and
     *            optional jini overrides.
     */
    public JiniServicesHelper(final String[] args) {

        if (args == null)
            throw new IllegalArgumentException();

        if (args.length == 0)
            throw new IllegalArgumentException();

        if (args[0] == null)
            throw new IllegalArgumentException();

        if (!new File(args[0]).exists()) {

            throw new RuntimeException("Configuration file not found: "
                    + args[0]);
            
        }
        
        this.args = args;

    }

    private final String[] args;

    /**
     * Thread pool used to run the various services.
     */
    private final ExecutorService threadPool = Executors
            .newCachedThreadPool(new DaemonThreadFactory(getClass().getName()
                    + ".threadPool"));
    
    private final IServiceListener serviceListener = new ServiceListener();

    /**
     * The directory in which all federation state is located.
     */
    private File fedServiceDir;

    /**
     * The zookeeper client port chosen by {@link #start()}.
     */
    private int clientPort;
    
    /**
     * The directory in which zookeeper is running.
     */
    private File zooDataDir;

    /**
     * Starts all services and connects the {@link JiniClient} to the
     * federation.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     * @throws RuntimeException
     *             if something goes wrong, typically a configuration error or
     *             you did not set the security policy.
     * 
     * @todo Start/stop jini. This class does not know where jini was installed
     *       and is therefore unable to start it automatically. The bundled
     *       configuration for jini uses variables. An alternative bundled
     *       configuration could be devised without those variables or by
     *       binding them based on the root of the source tree.
     * 
     * @todo not reentrant, but should throw exception if called twice.
     */
    public void start() throws InterruptedException, ConfigurationException {

        try {

            innerStart();
            
        } catch (Throwable t) {
            
            try {
            
                destroy();
            
            } catch (Throwable t2) {
                
                log.error("Shutdown error: " + t2, t2);
                
            }

            // throw the startup error, not any possible shutdown error.
            throw new RuntimeException("Startup error: " + t, t);
            
        }

    }

    /**
     * Invoked by {@link #start()}.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     * @throws KeeperException
     */
    private void innerStart() throws InterruptedException,
            ConfigurationException, KeeperException {

        System.setSecurityManager(new SecurityManager());

        /*
         * Pull some fields out of the configuration file that we need to
         * set things up.
         */
        
        // all services will be located in children of this directory.
        final File fedServiceDir;

        // the federation name.
        final String fedname;
        {
          
            final Configuration config = ConfigurationProvider
                    .getInstance(args);

            fedname = (String) config.getEntry("bigdata", "fedname",
                    String.class);

            fedServiceDir = (File) config.getEntry("bigdata", "serviceDir",
                    File.class);

            if (fedServiceDir.getPath().equals(".")) {

                throw new RuntimeException(
                        "Startup directory MUST NOT be the current directory.");

            }

            System.err.println("fedname=" + fedname);

            System.err.println("fedServiceDir=" + fedServiceDir);

            this.fedServiceDir = fedServiceDir;
            
        }

        /*
         * Setup a zookeeper instance.
         * 
         * Note: [options] contains the overrides that we need to connect to the
         * right zookeeper instance. This is very important! These options need
         * to be used when we start the client or any of the services within
         * this method so they use the right zookeeper instance.
         */
        final String[] options;
        {

            // the zookeeper service directory.
            zooDataDir = new File(fedServiceDir, "zookeeper");
            
            if(zooDataDir.exists()) {
                
                // clear out old zookeeper state first.
                recursiveDelete(zooDataDir);
                
            }
            
            // create.
            zooDataDir.mkdirs();

            try {

                // find ports that are not in use.
                clientPort = getPort(2181/* suggestedPort */);
                final int peerPort = getPort(2888/* suggestedPort */);
                final int leaderPort = getPort(3888/* suggestedPort */);
                final String servers = "1=localhost:" + peerPort + ":"
                        + leaderPort;

                options = new String[] {
                        // overrides the clientPort to be unique.
                        QuorumPeerMain.class.getName()
                                + "."
                                + ZookeeperServerConfiguration.Options.CLIENT_PORT
                                + "=" + clientPort,
                        // overrides servers declaration.
                        QuorumPeerMain.class.getName() + "."
                                + ZookeeperServerConfiguration.Options.SERVERS
                                + "=\"" + servers + "\"",
                        // overrides the dataDir
                        QuorumPeerMain.class.getName() + "."
                                + ZookeeperServerConfiguration.Options.DATA_DIR
                                + "=new java.io.File("
                                + ConfigMath.q(zooDataDir.toString()) + ")"//
                };
                
                System.err.println("options=" + Arrays.toString(options));

                final Configuration config = ConfigurationProvider
                        .getInstance(concat(args, options));

                // start zookeeper (a server instance).
                final int nstarted = ZookeeperProcessHelper.startZookeeper(
                        config, serviceListener);

                if (nstarted != 1) {

                    throw new RuntimeException(
                            "Expected to start one zookeeper instance, not "
                                    + nstarted);
                    
                }

            } catch (Throwable t) {

                // don't leave around the dataDir if the setup fails.
                recursiveDelete(zooDataDir);

                throw new RuntimeException(t);

            }

        }

        /*
         * COnnect the client and wait until zookeeper is up.
         */
        {

            // new client.
            client = JiniClient.newInstance(concat(args, options));

            // connect the client - this will get discovery running.
            final JiniFederation<?> fed = client.connect();

            if (!fed.getZookeeperAccessor().awaitZookeeperConnected(1000,
                    TimeUnit.MILLISECONDS)) {

                throw new RuntimeException("Zookeeper client not connected.");

            }

//            zookeeper = fed.getZookeeper();

//            zooConfig = fed.getZooConfig();

            fed.createKeyZNodes(fed.getZookeeper());
            
        }
        
        {

            final File serviceDir = new File(fedServiceDir, "ds1");

            final File dataDir = new File(serviceDir, "data");

            final String[] overrides = new String[] {
                    /*
                     * Override the service directory.
                     */
                    DataServer.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File("
                            + ConfigMath.q(ConfigMath
                                    .getAbsolutePath(serviceDir)) + ")",
                    /*
                     * Override the data directory. Since we are not shelling
                     * out a new JVM, the current working directory when the
                     * service starts will be the same directory in which this
                     * JVM was started. Therefore we must specify an absolution
                     * directory.
                     */
                    DataServer.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "DataServer.Options.DATA_DIR" + ", "
                            + ConfigMath.q(ConfigMath.getAbsolutePath(dataDir))
                            + ")\n" +
                            //
                            "}\n" };

            System.err.println("overrides=" + Arrays.toString(overrides));

            threadPool.execute(dataServer1 = new DataServer(concat(args,
                    concat(overrides, options)), new FakeLifeCycle()));

        }

        {

            final File serviceDir = new File(fedServiceDir, "ds0");

            final File dataDir = new File(serviceDir, "data");

            final String[] overrides = new String[] {
                    /*
                     * Override the service directory.
                     */
                    DataServer.class.getName()
                            + "."
                            + AbstractServer.ConfigurationOptions.SERVICE_DIR
                            + "=new java.io.File("
                            + ConfigMath.q(ConfigMath
                                    .getAbsolutePath(serviceDir)) + ")",
                    /*
                     * Override the data directory. Since we are not shelling
                     * out a new JVM, the current working directory when the
                     * service starts will be the same directory in which this
                     * JVM was started. Therefore we must specify an absolution
                     * directory.
                     */
                    DataServer.class.getName()
                            + ".properties = new com.bigdata.util.NV[] {\n"
                            +
                            //
                            " new NV(" + "DataServer.Options.DATA_DIR" + ", "
                            + ConfigMath.q(ConfigMath.getAbsolutePath(dataDir))
                            + ")\n" +
                            //
                            "}\n" };

            System.err.println("overrides=" + Arrays.toString(overrides));

            threadPool.execute(dataServer0 = new DataServer(concat(args,
                    concat(overrides, options)), new FakeLifeCycle()));

        }

        threadPool.execute(clientServer0 = new ClientServer(concat(
                args, options), new FakeLifeCycle()));

        threadPool.execute(transactionServer0 = new TransactionServer(concat(
                args, options), new FakeLifeCycle()));

        threadPool.execute(metadataServer0 = new MetadataServer(concat(args,
                options), new FakeLifeCycle()));

        threadPool.execute(loadBalancerServer0 = new LoadBalancerServer(concat(
                args, options), new FakeLifeCycle()));

        // Wait until all the services are up.
        getServiceID(clientServer0);
        getServiceID(transactionServer0);
        getServiceID(metadataServer0);
        getServiceID(dataServer0);
        getServiceID(dataServer1);
        getServiceID(loadBalancerServer0);

    }

    /**
     * Shuts down and <em>destroys</em> the services in the federation. The
     * shutdown is abrupt. You can expect to see messages about interrupted IO
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

        ZooKeeper zookeeper = null;
        
        ZookeeperClientConfig zooConfig = null;
        
        if (client != null && client.isConnected()) {

            zooConfig = client.getFederation().getZooConfig();
            
            zookeeper = client.getFederation().getZookeeper();
            
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

        if (clientServer0 != null) {

            clientServer0.destroy();

            clientServer0 = null;

        }
        
        if (loadBalancerServer0 != null) {

            loadBalancerServer0.destroy();

            loadBalancerServer0 = null;

        }

        if (transactionServer0 != null) {

            transactionServer0.destroy();

            transactionServer0 = null;

        }

        if (zookeeper != null && zooConfig != null) {

            try {

                // clear out everything in zookeeper for this federation.
                zookeeper.delete(zooConfig.zroot, -1/* version */);
                
            } catch (Exception e) {
                
                // ignore.
                log.warn("zroot=" + zooConfig.zroot + " : "
                        + e.getLocalizedMessage(), e);
                
            }
            
        }
        
        try {
         
            ZooHelper.kill(clientPort);
            
        } catch (Throwable t) {
            log.error("Could not kill zookeeper: clientPort=" + clientPort
                    + " : " + t, t);
        }

        if (zooDataDir != null && zooDataDir.exists()) {

            /*
             * Wait a bit and then try and delete the zookeeper directory.
             */
            
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            recursiveDelete(zooDataDir);

        }
        
        if (fedServiceDir != null && fedServiceDir.exists()) {

            fedServiceDir.delete();
            
        }
        
        threadPool.shutdownNow();
        
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
    static private ServiceID getServiceID(final AbstractServer server) {

        ServiceID serviceID = null;

        for (int i = 0; i < 20 && serviceID == null; i++) {

            /*
             * Note: This can be null since the serviceID is not assigned
             * synchronously by the registrar.
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
            throw new RuntimeException("Server did not start? "+server);

        return serviceID;

    }


    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    protected static int getPort(final int suggestedPort) throws IOException {
        
        ServerSocket openSocket;
        
        try {
        
            openSocket = new ServerSocket(suggestedPort);
            
        } catch (BindException ex) {
            
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        
        }

        final int port = openSocket.getLocalPort();
        
        openSocket.close();

        return port;
        
    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * <p>
     * Note: Files that are not recognized will be logged by the
     * {@link ResourceFileFilter}.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();

            if (children == null) {

                // The directory does not exist.
                return;
                
            }
            
            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if(log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * Mock implementation used by some unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class ServiceListener implements IServiceListener {

        public Queue<ProcessHelper> running = new ConcurrentLinkedQueue<ProcessHelper>();

        public void add(ProcessHelper service) {

            if (log.isInfoEnabled())
                log.info("adding: " + service);

            running.add(service);

        }

        public void remove(ProcessHelper service) {

            if (log.isInfoEnabled())
                log.info("removing: " + service);

            running.remove(service);

        }

    }
    
}
