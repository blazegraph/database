/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.journal.jini.ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.rmi.Remote;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.AssertionFailedError;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.system.SystemUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.IndexManagerCallable;
import com.bigdata.ha.RunState;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.JavaServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration.AbstractServiceStarter;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.util.JiniUtil;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.StoreState;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.quorum.AbstractQuorumClient;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumClient;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.zk.ZKQuorumClient;
import com.bigdata.quorum.zk.ZKQuorumImpl;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.RemoteDestroyAdmin;
import com.bigdata.util.InnerCause;
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.DumpZookeeper;
import com.bigdata.zookeeper.ZooHelper;
import com.bigdata.zookeeper.start.config.ZookeeperClientConfig;
import com.bigdata.zookeeper.util.ConfigMath;

/**
 * Class layers in support to start and stop the {@link HAJournalServer}
 * processes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractHA3JournalServerTestCase extends
        AbstractHAJournalServerTestCase implements DiscoveryListener {

    /** Quorum client used to monitor (or act on) the logical service quorum. */
    protected Quorum<HAGlue, QuorumClient<HAGlue>> quorum = null;
    
    public AbstractHA3JournalServerTestCase() {
    }

    public AbstractHA3JournalServerTestCase(final String name) {
        super(name);
    }

    /**
     * The timeout in milliseconds to await the discovery of a service if there
     * is a cache miss (default {@value #DEFAULT_CACHE_MISS_TIMEOUT}).
     */
    static final protected long cacheMissTimeout = 2000;
    
    /**
     * Implementation listens for the death of the child process and can be used
     * to decide when the child process is no longer executing.
     */
    static class ServiceListener implements IServiceListener {

        @SuppressWarnings("unused")
        private volatile HAGlue haGlue;
        private volatile ProcessHelper processHelper;
        private volatile boolean dead = false;
        private volatile int childPID = 0;
        
        public ServiceListener() {
            
        }

        public void setService(final HAGlue haGlue) {

            if (haGlue == null)
                throw new IllegalArgumentException();

            this.haGlue = haGlue;
        }

//        @SuppressWarnings("unused")
//        public HAGlue getHAGlue() {
//
//            return haGlue;
//            
//        }

        @Override
        public void add(final ProcessHelper processHelper) {

            if (processHelper == null)
                throw new IllegalArgumentException();

            this.processHelper = processHelper;

        }

        public void remove(final ProcessHelper processHelper) {

            if (processHelper == null)
                throw new IllegalArgumentException();

            if (processHelper != this.processHelper)
                throw new AssertionError();

            /*
             * Note: Do not clear the [processHelper] field.
             */

            // Mark the process as known dead.
            dead = true;

        }

        public ProcessHelper getProcessHelper() {
        
            return processHelper;
            
        }
        
        public boolean isDead() {

            return dead;

        }

    }

    /**
     * Return any overrides to be specified together with the basic
     * {@link Configuration}.  Each override is the fully qualified name
     * of a {@link Configuration} parameter together with its value. For
     * example:
     * <pre>
     * com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new NoSnapshotPolicy();
     * </pre>
     */
    protected String[] getOverrides() {
        
        return new String[]{};
        
    }
    
    /**
     * The {@link Remote} interfaces for these services (if started and
     * successfully discovered).
     */
    protected HAGlue serverA = null;

	protected HAGlue serverB = null;

	protected HAGlue serverC = null;

    /**
     * {@link UUID}s for the {@link HAJournalServer}s.
     */
    private UUID serverAId = UUID.randomUUID();

	private UUID serverBId = UUID.randomUUID();

	private UUID serverCId = UUID.randomUUID();

    /**
     * The HTTP ports at which the services will respond.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/730" > Allow configuration
     *      of embedded NSS jetty server using jetty-web.xml </a>
     */
    protected final int A_JETTY_PORT = 8090;
	protected final int B_JETTY_PORT = A_JETTY_PORT + 1;
	protected final int C_JETTY_PORT = B_JETTY_PORT + 1;

    /**
     * These {@link IServiceListener}s are used to reliably detect that the
     * corresponding process starts and (most importantly) that it is really
     * dies once it has been shutdown or destroyed.
     */
    protected ServiceListener serviceListenerA = null;

	protected ServiceListener serviceListenerB = null;

    protected ServiceListener serviceListenerC = null;
    
    private LookupDiscoveryManager lookupDiscoveryManager = null;

    private ServiceDiscoveryManager serviceDiscoveryManager = null;

    private HAGlueServicesClient discoveryClient = null;
    
//    /**
//     * The {@link ZooKeeperAccessor} used by the {@link #quorum}.
//     */
//    private ZooKeeperAccessor zka = null;
    
    /**
     * The {@link ZooKeeper} instance used by the {@link #quorum}.
     */
    private ZooKeeper zookeeper = null;
    
    private ZookeeperClientConfig zkClientConfig = null;
    
//    /**
//     * The {@link ACL}s used by the {@link #quorum}.
//     */
//    private List<ACL> acl = null;

    /**
     * The {@link ZooKeeper} instance used by the {@link #quorum}.
     */
    private ZooKeeper getZookeeper() {
        final ZooKeeper zookeeper = this.zookeeper;
        if (zookeeper == null)
            throw new IllegalStateException();
        return zookeeper;
    }
    
    /**
     * Return the negotiated zookeeper session timeout in milliseconds (if
     * available) and otherwise the requested zookeeper session timeout.
     */
    protected int getZKSessionTimeout() {
        final ZooKeeper zookeeper = this.zookeeper;
        if (zookeeper != null) {
            final int t = zookeeper.getSessionTimeout();
            if (t > 0)
                return t;
        }
        ZookeeperClientConfig t = zkClientConfig;
        if (t != null) {
            return t.sessionTimeout;
        }
        return 10000;
    }
    
    /**
     * The logicalServiceId (without the zroot prefix).
     */
    private String logicalServiceId = null;
    
    /**
     * The zpath of the logical service.
     */
    protected String logicalServiceZPath = null;
    
    @Override
    protected void setUp() throws Exception {
        
        /*
         * Destroy the test directory structure.
         * 
         * Note: This is done before we run the test rather than after so we can
         * look at the end state of the data after running the test.
         */
        {

            final File testDir = getTestDir();

            if (testDir.exists()) {

                recursiveDelete(testDir);

            }

        }

        super.setUp();

//        /*
//         * Some tests tear down the zookeeper server process. This ensures that
//         * we restart that server process before running another test.
//         */
//        try {
//            assertZookeeperRunning();
//        } catch (AssertionFailedError ex) {
//            try {
//                log.warn("Forcing ZooKeeper server process restart.");
//                startZookeeper();
//            } catch (RuntimeException ex2) {
//                log.error(ex2, ex2);
//            }
//        }

        // Unique for each test.
        logicalServiceId = "CI-HAJournal-" + getName() + "-" + UUID.randomUUID();

        /*
         * Read the jini/river configuration file. We need this to setup the
         * clients that we will use to lookup the services that we start.
         */
        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { SRC_PATH + "jiniClient.config" });

        final JiniClientConfig jiniClientConfig = new JiniClientConfig(
                JiniClientConfig.Options.NAMESPACE, config);

        /*
         * Note: This class will perform multicast discovery if ALL_GROUPS
         * is specified and otherwise requires you to specify one or more
         * unicast locators (URIs of hosts running discovery services). As
         * an alternative, you can use LookupDiscovery, which always does
         * multicast discovery.
         */
        lookupDiscoveryManager = new LookupDiscoveryManager(
                jiniClientConfig.groups, jiniClientConfig.locators,
                this /* DiscoveryListener */, config);

        /*
         * Setup a helper class that will be notified as services join or leave
         * the various registrars to which the data server is listening.
         */
        serviceDiscoveryManager = new ServiceDiscoveryManager(
                lookupDiscoveryManager, new LeaseRenewalManager(), config);

        // Setup discovery for HAGlue clients.
        discoveryClient = new HAGlueServicesClient(serviceDiscoveryManager,
                null/* serviceDiscoveryListener */, cacheMissTimeout);

        if (!isZookeeperRunning()) {

            /*
             * Ensure that zookeeper is running.
             * 
             * Note: Some unit tests will tear down the zookeeper server
             * process. This ensures that the zookeeper server process is
             * restarted before the next test runs.
             */

            startZookeeper();

        }
        
        // Setup quorum client.
        quorum = newQuorum();

    }
    
    @Override
    protected void tearDown() throws Exception {

         if (quorum != null && log.isInfoEnabled()) {

            /*
             * Echo the final quorum state (as currently reflected).
             */

            log.info(quorum.toString());

        }
        
        if (zookeeper != null && log.isInfoEnabled()) {

            /*
             * Dump the final zookeeper state for the logical service.
             */

            log.info("Zookeeper State for logical service: \n" + dumpZoo());

        }

        destroyAll();
                
        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.terminate();
            serviceDiscoveryManager = null;
        }

        if (lookupDiscoveryManager != null) {
            lookupDiscoveryManager.terminate();
            lookupDiscoveryManager = null;
        }

        if (discoveryClient != null) {
            discoveryClient.terminate();
            discoveryClient = null;
        }

        if (quorum != null) {
            quorum.terminate();
            quorum = null;
        }

        if (zookeeper != null) {
            final String zroot = logicalServiceZPath;
            destroyZNodes(zroot, zookeeper);
            zookeeper.close();
            zookeeper = null;
            zkClientConfig = null;
        }

        logicalServiceId = null;
        logicalServiceZPath = null;
        serverAId = serverBId = serverCId = null;

       super.tearDown();

    }

    protected void destroyAll() throws AsynchronousQuorumCloseException,
            InterruptedException, TimeoutException {
        /**
         * The most reliable tear down is in reverse pipeline order.
         * 
         * This may not be necessary long term but for now we want to avoid
         * destroying the leader first since it can lead to problems as
         * followers attempt to reform
         */
        final HAGlue leader;
        final File leaderServiceDir;
        final ServiceListener leaderListener;
        if (quorum.isQuorumMet()) {
            final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                    TimeUnit.MILLISECONDS);
            /*
             * Note: It is possible to resolve a proxy for a service that
             * has been recently shutdown or destroyed.  This is effectively
             * a data race.
             */
            final HAGlue t = quorum.getClient().getLeader(token);
            if (t.equals(serverA)) {
                leader = t;
                leaderServiceDir = getServiceDirA();
                leaderListener = serviceListenerA;
            } else if (t.equals(serverB)) {
                leader = t;
                leaderServiceDir = getServiceDirB();
                leaderListener = serviceListenerB;
            } else if (t.equals(serverC)) {
                leader = t;
                leaderServiceDir = getServiceDirC();
                leaderListener = serviceListenerC;
            } else {
                if (serverA == null && serverB == null && serverC == null) {
                    /*
                     * There are no services running and nothing to shutdown. We
                     * probably resolved a stale proxy to the leader above.
                     */
                    return;
                }
                throw new IllegalStateException(
                        "Leader is none of A, B, or C: leader=" + t + ", A="
                                + serverA + ", B=" + serverB + ", C=" + serverC);
            }
        } else {
            leader = null;
            leaderServiceDir = null;
            leaderListener = null;
        }
        
        if (leader == null || !leader.equals(serverA)) {
            destroyA();
        }

        if (leader == null || !leader.equals(serverB)) {
            destroyB();
        }

        if (leader == null || !leader.equals(serverC)) {
            destroyC();
        }
        
        // Destroy leader last
        if (leader != null) {
            safeDestroy(leader, leaderServiceDir, leaderListener);
            
            serverA = serverB = serverC = null;
            serviceListenerA = serviceListenerC =serviceListenerB = null;
        }

        
    }

    protected UUID[] getServices(final HAGlue[] members) throws IOException {
        final UUID[] services = new UUID[members.length];
        for (int m = 0; m < members.length; m++) {
            services[m] = members[m].getServiceId();
        }
        
        return services;
    }
    
    /**
     * Waits for pipeline in expected order
     * 
     * @param members
     * @throws IOException
     */
    protected void awaitPipeline(final HAGlue[] members) throws IOException {
        awaitPipeline(5, TimeUnit.SECONDS, members);
    }

    /**
     * Waits for pipeline in expected order
     * 
     * @param members
     * @throws IOException
     */
    protected void awaitPipeline(final long timeout, final TimeUnit unit,
            final HAGlue[] members) throws IOException {

        final UUID[] services = getServices(members);

        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals(services, quorum.getPipeline());
                } catch (Exception e) {
                    // KB does not exist.
                    fail();
                }
            }

        }, timeout, unit);
        
    }
    
    protected void assertReady(final HAGlue[] members) throws IOException {

        for (HAGlue member : members) {
        
            final HAStatusEnum status = member.getHAStatus();
            
            if (log.isInfoEnabled())
                log.info(member.getServiceName() + ": " + status);
            
            assertFalse(HAStatusEnum.NotReady == status);
            
        }
        
    }
    
    /**
     * Waits for joined in expected order
     * 
     * @param members
     * @throws IOException
     */
    protected void awaitJoined(final HAGlue[] members) throws IOException {
        
        final UUID[] services = getServices(members);
        
        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals(services, quorum.getJoined());
                } catch (Exception e) {
                    // KB does not exist.
                    fail();
                }
            }

        }, 5, TimeUnit.SECONDS);
        
    }
    
    /**
     * Waits for members in expected order
     * 
     * @param members
     * @throws IOException
     */
    protected void awaitMembers(final HAGlue[] members) throws IOException {
        
        final UUID[] services = getServices(members);
        
        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    assertEquals(services, quorum.getMembers());
                } catch (Exception e) {
                    // KB does not exist.
                    fail();
                }
            }

        }, 5, TimeUnit.SECONDS);
        
    }

    /**
     * Start A then B then C. As each service starts, this method waits for that
     * service to appear in the pipeline in the proper position.
     * 
     * @return The ordered array of services <code>[A, B, C]</code>
     */
    protected HAGlue[] startSequenceABC() throws Exception {

        startA();
        awaitPipeline(new HAGlue[] { serverA });
        
        startB();
        awaitPipeline(new HAGlue[] { serverA, serverB });
        
        startC();
        awaitPipeline(new HAGlue[] { serverA, serverB, serverC });

        return new HAGlue[] { serverA, serverB, serverC };
        
    }
    
    /*
     * Utility methods to access service HALog file directories
     */
    
    /**
     * The effective name for this test as used to name the directories in which
     * we store things.
     */
    protected String getEffectiveTestFileName() {
        
        return effectiveTestFileName;
        
    }

    /**
     * The effective name for this test as used to name the directories in which
     * we store things.
     * 
     * TODO If there are method name collisions across the different test
     * classes then the test suite name can be added to this. Also, if there are
     * file naming problems, then this value can be munged before it is
     * returned.
     */
    private final String effectiveTestFileName = getClass().getSimpleName()
            + "." + getName();

    /**
     * The directory that is the parent of each {@link HAJournalServer}'s
     * individual service directory.
     */
    protected File getTestDir() {
        return new File(TGT_PATH, getEffectiveTestFileName());
    }

    protected File getServiceDirA() {
        return new File(getTestDir(), "A");
    }
    
    protected File getServiceDirB() {
        return new File(getTestDir(), "B");
    }
    
    protected File getServiceDirC() {
        return new File(getTestDir(), "C");
    }
    
    protected File getHAJournalFileA() {
        return new File(getServiceDirA(), "bigdata-ha.jnl");
    }

    protected File getHAJournalFileB() {
        return new File(getServiceDirB(), "bigdata-ha.jnl");
    }

    protected File getHAJournalFileC() {
        return new File(getServiceDirC(), "bigdata-ha.jnl");
    }

    protected File getHALogDirA() {
        return new File(getServiceDirA(), "HALog");
    }

    protected File getHALogDirB() {
        return new File(getServiceDirB(), "HALog");
    }

    protected File getHALogDirC() {
        return new File(getServiceDirC(), "HALog");
    }

    protected File getSnapshotDirA() {
        return new File(getServiceDirA(), "snapshot");
    }

    protected File getSnapshotDirB() {
        return new File(getServiceDirB(), "snapshot");
    }

    protected File getSnapshotDirC() {
        return new File(getServiceDirC(), "snapshot");
    }
    
   /**
     * Clear out everything in zookeeper for the specified zpath.
     */
    private void destroyZNodes(final String zpath, final ZooKeeper zookeeper) {

        if (log.isInfoEnabled())
            log.info("zpath=" + zpath);
        
        try {

            if (zookeeper.exists(zpath, false/* watch */) != null) {

                ZooHelper.destroyZNodes(zookeeper, zpath, 0/* depth */);
            }

        } catch (InterruptedException ex) {

            log.warn(ex);

        } catch (SessionExpiredException ex) {

            log.warn(ex);

        } catch (ConnectionLossException ex) {

            log.warn(ex);

        } catch (Exception e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Dump the zookeeper state for the logical service.
     */
    protected String dumpZoo() throws KeeperException, InterruptedException {

        final StringWriter sw = new StringWriter();

        final PrintWriter w = new PrintWriter(sw);

        final ZooKeeper zk = getZookeeper();
        try {
            new DumpZookeeper(zk).dump(w, true/* showData */,
                    logicalServiceZPath, 0/* depth */);
        } catch (KeeperException ex) {
            /*
             * Note: This is expected if you have shut down the zookeeper server
             * process under test suite control.
             */
            ex.printStackTrace(w);
        }
        w.flush();
        w.close();

        sw.flush();

        return sw.toString();

    }
    
    /**
     * Safely destroy the service.
     * 
     * @param haGlue
     *            The service.
     */
    protected void safeDestroy(final HAGlue haGlue, final File serviceDir,
            final ServiceListener serviceListener) {

        if (log.isInfoEnabled())
            log.info("Destroying service: " + haGlue + ", serviceDir="
                    + serviceDir);

        if (haGlue == null) {

            tidyServiceDirectory(serviceDir); // ensure empty

            return;
            
        }

        try {

            final UUID serviceId = haGlue.getServiceUUID();

            haGlue.destroy();

            awaitServiceGone(serviceId, haGlue, serviceDir, serviceListener);

        } catch (Throwable t) {
            
            if (InnerCause.isInnerCause(t, java.net.ConnectException.class)) {
            
                log.warn("Service is down (RMI not allowed): " + t);
                
            } else {
                
                // Some other problem.
                log.error(t, t);

            }
            
            {

                if (serviceListener != null && serviceListener.childPID != 0) {

                    final int childPID = serviceListener.childPID;

                    log.warn("Attempting to signal service: " + serviceDir
                            + ", pid=" + childPID);

                    // Try to request a thread dump.
                    if (trySignal(SignalEnum.QUIT, childPID)) {

                        // Give the child a moment to respond.
                        try {
                            Thread.sleep(2000/* ms */);
                        } catch (InterruptedException e) {
                            // Propagate the interrupt.
                            Thread.currentThread().interrupt();
                        }

                    }

                    // Sure kill on the child.
                    trySignal(SignalEnum.KILL, childPID);

                }

            }
                    
            // try and ensure serviceDir is tidied in any event
            // Remove *.jnl, HALog/*, snapshot/*
            log.warn("Need to clear directory explicitly: " + serviceDir.getAbsolutePath());
            
            tidyServiceDirectory(serviceDir);
        }

    }
    
    private void tidyServiceDirectory(final File serviceDir) {
        if (serviceDir == null || !serviceDir.exists())
            return;

        for (File file : serviceDir.listFiles()) {
            final String name = file.getName();

            if (name.endsWith(".jnl") || name.equals("snapshot")
                    || name.equals("HALog")) {
                recursiveDelete(file);
            }
        }
    }
    
    /**
     * Some signals understood by Java.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    protected enum SignalEnum {
      /** Heap dump. */
      INT(2,"INT"),
      /** Thread dump (^C). */
      QUIT(3,"QUIT"),
      /** Sure kill. */
      KILL(9,"KILL");
      private final int code;
      private final String symbol;
      private SignalEnum(int code, String symbol) {
          this.code = code;
          this.symbol = symbol;
      }
      public int code() {
          return code;
      }
      public String symbol() {
          return symbol;
      }
    };

    /**
     * Attempt to signal a process (presumably a child process).
     * 
     * @param signalEnum
     *            The signal.
     * @param pid
     *            The pid of the process.
     *            
     * @return true unless an exception was encountered (a <code>true</code>
     *         return is not a sure indicator of success).
     */
    static protected boolean trySignal(final SignalEnum signalEnum,
            final int pid) {

        final String cmd = "kill -" + signalEnum.symbol() + " " + pid;

        log.warn("SIGNAL: " + cmd);

        try {
        
            Runtime.getRuntime().exec(cmd);

            return true;
            
        } catch (IOException e) {

            log.error("FAILED: " + cmd, e);
            
            return false;

        }

    }
    
    protected void destroyA() {
        safeDestroy(serverA, getServiceDirA(), serviceListenerA);
        serverA = null;
        serviceListenerA = null;
    }

    protected void destroyB() {
        safeDestroy(serverB, getServiceDirB(), serviceListenerB);
        serverB = null;
        serviceListenerB = null;
    }

    protected void destroyC() {
        safeDestroy(serverC, getServiceDirC(), serviceListenerC);
        serverC = null;
        serviceListenerC = null;
    }

    protected void shutdownA() throws IOException {
        safeShutdown(serverA, getServiceDirA(), serviceListenerA, true);

        serverA = null;
        serviceListenerA = null;
    }

    protected void shutdownB() throws IOException {
        safeShutdown(serverB, getServiceDirB(), serviceListenerB, true);

        serverB = null;
        serviceListenerB = null;
    }

    protected void shutdownC() throws IOException {
        safeShutdown(serverC, getServiceDirC(), serviceListenerC, true);

        serverC = null;
        serviceListenerC = null;
    }

    protected void kill(final HAGlue service) throws IOException {

        final int pid = ((HAGlueTest) service).getPID();

        trySignal(SignalEnum.KILL, pid);

    }

    /**
     * NOTE: This relies on equals() being valid for Proxies which isn't
     * necessarily something we should rely on
     */
    protected void shutdown(final HAGlue service) throws IOException {
        if (service == null) {
            throw new IllegalArgumentException();
        }

        if (service.equals(serverA)) {
            shutdownA();
        } else if (service.equals(serverB)) {
            shutdownB();
        } else if (service.equals(serverC)) {
            shutdownC();
        } else {
            throw new IllegalArgumentException("Unable to match service: "
                    + service + " possible problem with equals() on Proxy");
        }
    }
    
    protected void shutdownLeader() throws AsynchronousQuorumCloseException,
            InterruptedException, TimeoutException, IOException {

        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        final HAGlue leader = quorum.getClient().getLeader(token);

        shutdown(leader);
        
    }

    protected class SafeShutdownTask implements Callable<Void> {

        private final HAGlue haGlue;
        private final File serviceDir;
        private final ServiceListener serviceListener;
        private final boolean now;

        public SafeShutdownTask(final HAGlue haGlue, final File serviceDir,
                final ServiceListener serviceListener) {

            this(haGlue, serviceDir, serviceListener, false/* now */);

        }

        public SafeShutdownTask(final HAGlue haGlue, final File serviceDir,
                final ServiceListener serviceListener, final boolean now) {

            this.haGlue = haGlue;
            this.serviceDir = serviceDir;
            this.serviceListener = serviceListener;
            this.now = now;
            
        }
        
        @Override
        public Void call() {
            
            safeShutdown(haGlue, serviceDir, serviceListener, now);
            
            return null;
            
        }
        
    }
    
    protected class SafeShutdownATask extends SafeShutdownTask {

        public SafeShutdownATask() {
            this(false/* now */);
        }

        public SafeShutdownATask(final boolean now) {
            super(serverA, getServiceDirA(), serviceListenerA, now);
        }

    }

    protected class SafeShutdownBTask extends SafeShutdownTask {

        public SafeShutdownBTask() {
            this(false/* now */);
        }

        public SafeShutdownBTask(final boolean now) {
            super(serverB, getServiceDirB(), serviceListenerB, now);
        }

    }

    protected class SafeShutdownCTask extends SafeShutdownTask {

        public SafeShutdownCTask() {
            this(false/* now */);
        }

        public SafeShutdownCTask(final boolean now) {
            super(serverC, getServiceDirC(), serviceListenerC, now);
        }

    }

    /**
     * Debug class to explicitly ask one service to remove another.
     * 
     * This emulates the behaviour of the service in receiving correct notification
     * of a target service failure -for example after a wire pull or sure kill.
     * 
     */
    protected static class ForceRemoveService extends IndexManagerCallable<Void> {

        private static final long serialVersionUID = 1L;
        private final UUID service;

        ForceRemoveService(final UUID service) {
            this.service = service;
        }
        
        @Override
        public Void call() throws Exception {

            final HAJournal ha = (HAJournal) this.getIndexManager();
            
            ha.getQuorum().getActor().forceRemoveService(service);
            
            return null;
        }
        
    }

    void safeShutdown(final HAGlue haGlue, final File serviceDir,
            final ServiceListener serviceListener) {

        safeShutdown(haGlue, serviceDir, serviceListener, false/* now */);

    }
    
    protected void safeShutdown(final HAGlue haGlue, final File serviceDir,
            final ServiceListener serviceListener, final boolean now) {

        if (haGlue == null)
            return;

        try {

            final UUID serviceId = haGlue.getServiceUUID();
            
            // Shutdown the remote service.
            if (now)
                ((RemoteDestroyAdmin) haGlue).shutdownNow();
            else
                ((RemoteDestroyAdmin) haGlue).shutdown();
            
            awaitServiceGone(serviceId, haGlue, serviceDir, serviceListener);
            
        } catch (Throwable t) {
            
            if (InnerCause.isInnerCause(t, java.net.ConnectException.class)) {
            
                log.warn("Service is down: " + t);
                
            } else {
                
                // Some other problem.
                log.error(t, t);
                
            }
            
        }

    }

    /**
     * Await positive indication that the service is shutdown. This can mean
     * looking for the service {@link RunState} change, looking for the service
     * to no longer be discoverable, looking for the service to no longer accept
     * RMI requests, etc.
     * <p>
     * Note: If the child does not shutdown normally, then this will force a
     * kill of the child process and throw out an {@link AssertionFailedError}.
     * This {@link AssertionFailedError} is a good indication that there is a
     * problem with the process shutdown / destroy logic. You should obtain
     * thread dumps when this happens and examine them for the root cause of the
     * process failing to terminate normally.
     */
    private void awaitServiceGone(final UUID serviceId, final HAGlue haGlue,
            final File serviceDir, final ServiceListener serviceListener) {

        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    haGlue.getRunState();
                    fail();// still answering RMI requests.
                } catch (IOException e) {
                    // Service is down.
                    return;
                }
            }
        });

        assertCondition(new Runnable() {
            @Override
            public void run() {

                // try to discover the service item.
                final ServiceItem serviceItem = discoveryClient
                        .getServiceItem(serviceId);
                
                if (serviceItem != null) {
                 
                    // still registered.
                    fail();
               
                }
                
            }
        });

        try {

            assertCondition(new Runnable() {
                @Override
                public void run() {
                    // Wait for the process death.
                    assertTrue(serviceListener.isDead());
                }
            }, 10/* timeout */, TimeUnit.SECONDS);

        } catch (junit.framework.AssertionFailedError err) {
        
            /*
             * If we do not observe a normal process death, then attempt to kill
             * the child process.
             */

            try {

                final ProcessHelper processHelper = serviceListener
                        .getProcessHelper();

                if (processHelper != null) {

                    log.error("Forcing kill of child process.");

                    processHelper.kill(true/* immediateShutdown */);

                } else {

                    log.error("Child process not correctly terminated.");
                    
                }

            } catch (InterruptedException e) {
                
                // Ignore.
                
            }

            fail("Process did not die by itself: " + haGlue, err);

        }
        
        /**
         * Wait until the lock file is gone or is no longer locked.
         * 
         * FIXME try waiting for the .lock file to be unlocked. This might fix
         * the bounce and fail leader / follower tests. They test to fail with
         * traces like this
         * 
         * <pre>
         * ERROR: 09:01:34,209 334      main com.bigdata.Banner$1.uncaughtException(Banner.java:111): Uncaught exception in thread
         * java.lang.RuntimeException: Service already running: file=/Users/bryan/Documents/workspace/BIGDATA_READ_CACHE_HEAD/benchmark/CI-HAJournal-1/B/.lock
         *     at com.bigdata.journal.jini.ha.AbstractServer.acquireFileLock(AbstractServer.java:1118)
         *     at com.bigdata.journal.jini.ha.AbstractServer.<init>(AbstractServer.java:681)
         *     at com.bigdata.journal.jini.ha.HAJournalServer.<init>(HAJournalServer.java:472)
         *     at com.bigdata.journal.jini.ha.HAJournalServer.main(HAJournalServer.java:3602)
         * </pre>
         */
//        if(false)
//        assertCondition(new Runnable() {
//            public void run() {
//                try {
//                    
//                    final File lockFile = new File(serviceDir, ".lock");
//                    
//                    if (!lockFile.exists()) {
//                    
//                        // Lock file is gone.
//                        return;
//                        
//                    }
//
//                    RandomAccessFile lockFileRAF = new RandomAccessFile(
//                            lockFile, "rw");
//                    FileLock fileLock = null;
//                    try {
//
//                        fileLock = lockFileRAF.getChannel().tryLock();
//
//                        if (fileLock == null) {
//
//                            /*
//                             * Note: A null return indicates that someone else
//                             * holds the lock.
//                             */
//
//                            try {
//                                lockFileRAF.close();
//                            } catch (Throwable t) {
//                                // ignore.
//                            } finally {
//                                lockFileRAF = null;
//                            }
//
//                            throw new RuntimeException(
//                                    "Service still running: file=" + lockFile);
//
//                        }
//
//                    } catch (IOException ex) {
//
//                        /*
//                         * Note: This is true of NFS volumes.
//                         */
//
//                        log.warn("FileLock not supported: file=" + lockFile, ex);
//
//                    } finally {
//                        if (fileLock != null) {
//                            try {
//                                fileLock.close();
//                            } finally {
//                                fileLock = null;
//                            }
//                        }
//                        if (lockFileRAF != null) {
//                            try {
//                                lockFileRAF.close();
//                            } finally {
//                                lockFileRAF = null;
//                            }
//                        }
//                    }
//
//                    fail();// lock file still exists or is still locked.
//                } catch (IOException e) {
//                    // Service is down.
//                    return;
//                }
//            }
//        });

    }

    /**
     * Return the zookeeper client configuration file.
     */
    final protected String getZKConfigFile() {

        return "zkClient.config";
        
    }
    
    /**
     * The as-configured replication factor.
     * <p>
     * Note: This is defined in the HAJournal.config file, which is where the
     * {@link HAJournalServer} gets the correct value. We also need to have the
     * replicationFactor on hand for the test suite so we can setup the quorum
     * in the test fixture correctly. However, it is difficult to reach the
     * appropriate HAJournal.config file from the text fixture during
     * {@link #setUp()}. Therefore, for the test setup, this is achieved by
     * overriding this abstract method in the test class.
     */
    protected int replicationFactor() {

        return 3;
        
    }

    /**
     * Return Zookeeper quorum that can be used to reflect (or act on) the
     * distributed quorum state for the logical service.
     * 
     * @throws ConfigurationException
     * @throws InterruptedException 
     * @throws KeeperException 
     * @throws IOException 
     */
    protected Quorum<HAGlue, QuorumClient<HAGlue>> newQuorum()
            throws ConfigurationException, InterruptedException,
            KeeperException, IOException {

        final Configuration config = ConfigurationProvider
                .getInstance(new String[] { SRC_PATH + getZKConfigFile() });

        zkClientConfig = new ZookeeperClientConfig(config);

        final List<ACL> acl = zkClientConfig.acl;
        final String zoohosts = zkClientConfig.servers;
        final int sessionTimeout = zkClientConfig.sessionTimeout;

        // Note: Save reference.
        this.zookeeper = new ZooKeeper(zoohosts, sessionTimeout, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                if (log.isInfoEnabled())
                    log.info(event);
            }
        });
        /**
         * Wait until zookeeper is connected. Figure out how long that took. If
         * reverse DNS is not setup, then the following two tickets will prevent
         * the service from starting up in a timely manner. We detect this with
         * a delay of 4+ seconds before zookeeper becomes connected. This issue
         * does not appear in zookeeper 3.3.4.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1652
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1666
         */
        {
            final long begin = System.nanoTime();
            while (zookeeper.getState().isAlive()) {
                if (zookeeper.getState() == States.CONNECTED) {
                    // connected.
                    break;
                }
                final long elapsed = System.nanoTime() - begin;
                if (TimeUnit.NANOSECONDS.toSeconds(elapsed) > 10) {
                    fail("Either zookeeper is not running or reverse DNS is not configured. "
                            + "The ZooKeeper client is taking too long to resolve server(s): state="
                            + zookeeper.getState()
                            + ", config="
                            + zkClientConfig
                            + ", took="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
                }
                // wait and then retry.
                Thread.sleep(100/* ms */);
            }
        }
        
        // znode name for the logical service.
//        final String logicalServiceId = (String) config.getEntry(
//                ZookeeperClientConfig.Options.NAMESPACE,
//                ConfigurationOptions.LOGICAL_SERVICE_ID, String.class);
        final String logicalServiceId = getLogicalServiceId();

        final String logicalServiceZPathPrefix = zkClientConfig.zroot + "/"
                + HAJournalServer.class.getName();

        // zpath for the logical service (save reference).
        logicalServiceZPath = logicalServiceZPathPrefix + "/"
                + logicalServiceId;

        /**
         * Note: This is defined in the HAJournal.config file, which is where
         * the HAJournalServer gets the correct value.
         * 
         * However, we also need to have the replicationFactor on hand for the
         * test suite so we can setup the quorum in the test fixture correctly.
         */
        final int replicationFactor = replicationFactor();
//        {
//            replicationFactor = (Integer) config.getEntry(
//                    ConfigurationOptions.COMPONENT,
//                    ConfigurationOptions.REPLICATION_FACTOR, Integer.TYPE);
//        }

//        if (!zka.awaitZookeeperConnected(10, TimeUnit.SECONDS)) {
//
//            throw new RuntimeException("Could not connect to zk");
//
//        }
//
//        if (log.isInfoEnabled()) {
//            log.info("Connected to zookeeper");
//        }

        /*
         * Ensure key znodes exist.
         */
        try {
            getZookeeper().create(zkClientConfig.zroot,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            getZookeeper().create(logicalServiceZPathPrefix,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }
        try {
            getZookeeper().create(logicalServiceZPath,
                    new byte[] {/* data */}, acl, CreateMode.PERSISTENT);
        } catch (NodeExistsException ex) {
            // ignore.
        }

        // Quorum that can be used to monitor the distributed quorum state.
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Quorum<HAGlue, QuorumClient<HAGlue>> quorum = (Quorum) new ZKQuorumImpl<HAGlue, ZKQuorumClient<HAGlue>>(
                replicationFactor);//, zka, acl);

        quorum.start(new MockQuorumClient<HAGlue>(logicalServiceZPath));
        
        return quorum;
        
    }
    
    /**
     * Return the logicalServiceId. This is overridden to be include name of the
     * test and a {@link UUID} in order to keep HAJournalServer processes that
     * do not die nicely from causing crosstalk between the unit tests.
     */
    protected String getLogicalServiceId() {
    
        return logicalServiceId;

    }

    private class MockQuorumClient<S extends Remote> extends
            AbstractQuorumClient<S> implements ZKQuorumClient<S> {

        protected MockQuorumClient(String logicalServiceId) {

            super(logicalServiceId);
            
        }

        /**
         * Resolve an {@link HAGlue} object from its Service UUID.
         */
        @Override
        public S getService(final UUID serviceId) {

            final ServiceItem serviceItem = discoveryClient
                    .getServiceItem(serviceId);

            if (serviceItem == null) {

                // Not found (per the API).
                throw new QuorumException("Service not found: uuid="
                        + serviceId);

            }

            @SuppressWarnings("unchecked")
            final S service = (S) serviceItem.service;

            return service;

        }

        @Override
        public ZooKeeper getZooKeeper() {
            return zookeeper;
        }

        @Override
        public List<ACL> getACL() {
            final ZookeeperClientConfig t = zkClientConfig;
            return t == null ? null : t.acl;
        }

    }

    /**
     * Task to start an {@link HAJournalServer} in a new JVM.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    abstract class StartServerTask implements Callable<HAGlue> {

        private final String name;
        private final String configName;
        private final UUID serverId;
        private final int jettyPort;
        private final ServiceListener serviceListener;
        protected final boolean restart;

        public StartServerTask(final String name, final String configName,
                final UUID serverId, final int jettyPort,
                final ServiceListener serviceListener, final boolean restart) {

            this.name = name;
            this.configName = configName;
            this.serverId = serverId;
            this.jettyPort = jettyPort;
            this.serviceListener = serviceListener;
            this.restart = restart;

        }
        
        final public HAGlue start() throws Exception {

            return startServer(name, configName, serverId, jettyPort,
                    serviceListener, restart);

        }
        
    }

    protected class StartATask extends StartServerTask {

        public StartATask(final boolean restart) {

            super("A", "HAJournal-A.config", serverAId, A_JETTY_PORT,
                    serviceListenerA = new ServiceListener(), restart);

        }

        @Override
        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverA, getServiceDirA(), serviceListenerA);
                
                serverA = null;
                
            }
            
            return serverA = start();

        }

    }

    protected class StartBTask extends StartServerTask {

        public StartBTask(final boolean restart) {

            super("B", "HAJournal-B.config", serverBId, B_JETTY_PORT,
                    serviceListenerB = new ServiceListener(), restart);

        }

        @Override
        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverB, getServiceDirB(), serviceListenerB);
                
                serverB = null;
                
            }
            
            return serverB = start();

        }

    }

    protected class StartCTask extends StartServerTask {

        public StartCTask(final boolean restart) {

            super("C", "HAJournal-C.config", serverCId, C_JETTY_PORT,
                    serviceListenerC = new ServiceListener(), restart);

        }

        @Override
        public HAGlue call() throws Exception {

            if (restart) {

                safeShutdown(serverC, getServiceDirC(), serviceListenerC);
                
                serverC = null;
                
            }
            

            return serverC = start();

        }

    }

    protected HAGlue startA() throws Exception {

        return new StartATask(false/* restart */).call();

    }

    protected HAGlue startB() throws Exception {

        return new StartBTask(false/* restart */).call();

    }

    protected HAGlue startC() throws Exception {

        return new StartCTask(false/* restart */).call();

    }

    protected HAGlue restartA() throws Exception {

        return new StartATask(true/* restart */).call();

    }

    protected HAGlue restartB() throws Exception {

        return new StartBTask(true/* restart */).call();

    }

    protected HAGlue restartC() throws Exception {

        return new StartCTask(true/* restart */).call();

    }
    
    protected UUID getServiceAId() {
        return serverAId;
    }

    protected UUID getServiceBId() {
        return serverBId;
    }

    protected UUID getServiceCId() {
        return serverCId;
    }

    private HAGlue startServer(final String name,//
            final String sourceConfigFileName, //
            final UUID serviceId,//
            final int jettyPort,//
            final ServiceListener serviceListener,//
            final boolean restart) throws Exception {

        final String configFile = SRC_PATH + sourceConfigFileName;

        final File serviceDir = new File(getTestDir(), name);

        final String installedConfigFileName = "HAJournal.config";

        if (!serviceDir.exists()) {

            if (restart)
                fail("Not found: " + serviceDir);

            // Create service directory.
            serviceDir.mkdirs();

        }
        
        /*
         * Copy various configuration and accessory files into the service
         * directory.
         */
        if (!restart) {

            // security policy
            copyFile(new File("policy.all"),
                    new File(serviceDir, "policy.all"), false/* append */);

            /**
             * We need to setup a traditional webapp.
             * <p>
             * Note: We should test all of the behaviors of the web app as
             * deployed, assuming that we are using a standard WAR deployment
             * here.
             * <p>
             * Note: In fact, we don't really need to have the index.html page
             * for the existing test suite. Just web.xml and jetty.xml. Notice
             * that web.xml does not require any html resources unless you also
             * specify XHTML output for a SPARQL SELECT or the like. Right now,
             * it deploys a copy of everything in bigdata-war/src, so you could
             * write tests to the whole REST API and UX.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/730"
             *      > Allow configuration of embedded NSS jetty server using
             *      jetty-web.xml </a>
             */
            /*
             * Setup a copy of the webapp rooted on the service directory.
             * 
             * Note: We could do this with custom jetty.xml or web.xml files as
             * well. This is a bit brute force, but maybe it is more useful for
             * that.
             * 
             * TODO The webapp is being deployed to the serviceDir in order
             * to avoid complexities with the parent and child process paths
             * to the serviceDir and the webappDir.
             */
            {
                final File webAppDir = serviceDir;

                if (!webAppDir.exists() && !webAppDir.mkdirs()) {
                    throw new IOException("Could not create directory: "
                            + webAppDir);
                }

				copyFile(new File(WAR_DIR + WAR_FILE_NAME), new File(webAppDir,
						WAR_FILE_NAME), true);
				
				copyFile(
						new File(JETTY_OVERRIDE_DIR
								+ System.getProperty("file.separator")
								+ JETTY_OVERRIDE_FILE), new File(webAppDir,
								JETTY_OVERRIDE_FILE), true);
            }

            // log4j configuration.
            copyFile(new File(
                    "src/test/resources/logging/log4j-dev.properties"),
                    new File(serviceDir, "log4j-" + name + ".properties"),
                    false/* append */);

            // append log4j templates to get service specific log files.
            copyFile(new File(SRC_PATH,"log4j-template-"+name+".properties"),
                    new File(serviceDir, "log4j-" + name + ".properties"),
                    true/* append */);

            // java logging configuration.
            copyFile(new File(
                    "src/test/resources/logging/logging.properties"),
                    new File(serviceDir, "logging-" + name + ".properties"),
                    false/* append */);

            // HAJournalServer configuration
            copyFile(new File(SRC_PATH, sourceConfigFileName), //
                    new File(serviceDir, installedConfigFileName), false/* append */);

        }
        
        /*
         * Read jini configuration.
         */
        
        // Overrides for this test.
        final String[] testOverrides = getOverrides();

        // Add override for the serviceDir.
        final String[] overrides = ConfigMath.concat(
                new String[] { //
                    // The service directory.
                    "bigdata.serviceDir=new java.io.File(\"" + serviceDir + "\")",
//                    // Where to find jetty.xml
//                    HAJournalServer.ConfigurationOptions.COMPONENT + "."
//                    + HAJournalServer.ConfigurationOptions.JETTY_XML
//                    + "=\"bigdata-war/src/jetty.xml\"",
//                    + "=\"" + serviceDir + "/bigdata-war/src/jetty.xml\"",
                },
                testOverrides);

        // Config file + overrides from perspective of this JVM.
        final String[] ourArgs = ConfigMath.concat(new String[] { configFile },
                overrides);

        // Config file + overrides from perspective of the child process.
        final String[] childArgs = ConfigMath.concat(new String[] {
                installedConfigFileName, // as installed.
//                "bigdata.serviceDir=new java.io.File(\".\")" // relative to the serviceDir!
                }, testOverrides // plus anything from the test case.
                );

        final Configuration config = ConfigurationProvider.getInstance(ourArgs);
        
        final ServiceConfiguration serviceConfig = new HAJournalServerConfiguration(
                name, config, serviceId, jettyPort, /*serviceDir,*/ childArgs);

        final AbstractServiceStarter<?> serviceStarter = serviceConfig
                .newServiceStarter(serviceListener);

        final ProcessHelper processHelper = serviceStarter.call();

        try {

            if (log.isInfoEnabled())
                log.info("Awaiting service discovery: "
                        + processHelper.name);

            final ServiceID serviceID = JiniUtil.uuid2ServiceID(serviceId);

            final ServiceItem[] items = serviceDiscoveryManager.lookup(
                    new ServiceTemplate(//
                            serviceID, //
                            null, // iface[]
                            null //new Entry[]{new Name(name)}
                    ), // template
                    1, // minMatches
                    1, // maxMatches
                    null, // filter
                    5000 // timeout (ms)
                    );

            assertNotNull(items);

            assertTrue(items.length == 1);

            assertNotNull(items[0]);

            final ServiceItem serviceItem = items[0];

            final HAGlue haGlue = (HAGlue) serviceItem.service;

            // Set the HAGlue interface on the ServiceListener.
            serviceListener.setService(haGlue);

            try {

                // Have the child self-report its PID (best guess).
                serviceListener.childPID = ((HAGlueTest) haGlue).getPID();

                if (log.isInfoEnabled())
                    log.info("CHILD: name=" + name + ", childPID="
                            + serviceListener.childPID);

            } catch (IOException ex) {

                log.warn("Could not get childPID: " + ex, ex);

            }
            
            /*
             * Wait until the server is running.
             */
            assertCondition(new Runnable() {
                
                @Override
                public void run() {

                    try {

                        assertEquals(RunState.Running, haGlue.getRunState());
                        
                    } catch (IOException e) {
                        
                        throw new RuntimeException(e);
                        
                    }

                }
            }, 30, TimeUnit.SECONDS);

            
            return haGlue;

        } catch (Throwable t) {

            log.error(t, t);

            processHelper.kill(true/* immediateShutdown */);

            throw new ServiceStartException(
                    "Could not start/locate service: name=" + name
                            + ", configFile=" + configFile, t);

        }

    }
    
    /**
     * Exception thrown when the test harness could not start a service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected static class ServiceStartException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public ServiceStartException() {
            super();
        }

        public ServiceStartException(String message, Throwable cause) {
            super(message, cause);
        }

        public ServiceStartException(String message) {
            super(message);
        }

        public ServiceStartException(Throwable cause) {
            super(cause);
        }
        
    }

    /**
     * Copy a file
     * 
     * @param src
     *            The source file (must exist).
     * @param dst
     *            The target file.
     * 
     * @throws IOException
     */
    static protected void copyFile(final File src, final File dst,
            final boolean append) throws IOException {

        if (!src.exists())
            throw new FileNotFoundException(src.getAbsolutePath());

        if (log.isInfoEnabled())
            log.info("src=" + src + ", dst=" + dst + ", append=" + append);

        FileInputStream is = null;
        FileOutputStream os = null;
        try {
            is = new FileInputStream(src);
            os = new FileOutputStream(dst, append);
            copyStream(is, os);
            os.flush();
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException ex) {
                }
            if (os != null)
                try {
                    os.close();
                } catch (IOException ex) {
                }
        }
    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     *            
     * @throws IOException
     */
    static private void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {
        
            final int rdlen = content.read(buf);
            
            if (rdlen <= 0) {
            
                break;
                
            }
            
            outstr.write(buf, 0, rdlen);
            
        }

    }

//    /**
//     * Filter for the specific service item based on the Name Entry.
//     */
//    private static class NameItemFilter implements ServiceItemFilter {
//
//        final private String name;
//
//        public NameItemFilter(final String name) {
//
//            if (name == null)
//                throw new IllegalArgumentException();
//
//            this.name = name;
//            
//        }
//        
//        @Override
//        public boolean check(final ServiceItem serviceItem) {
//
//            final Entry[] entries = serviceItem.attributeSets;
//
//            String theName = null;
//
//            for (Entry e : entries) {
//
//                if (e instanceof Name && theName == null) {
//
//                    // found a name.
//                    theName = ((Name) e).name;
//
//                }
//            }
//
//            if (theName.equals(name))
//                log.info("Found: " + serviceItem);
//
//            return true;
//
//        }
//
//    }

    /**
     * Utility class for configuring and starting an {@link HAJournalServer}
     * under test suite control.
     */
    private class HAJournalServerConfiguration extends
            JavaServiceConfiguration {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        private final String serviceName;
        private final UUID serviceId;
        private final int jettyPort;
        /**
         * The value of this environment variable is passed down. You can set
         * this environment variable to force jetty to dump its internal start
         * after start.
         */
        private final boolean jettyDumpStart = Boolean
                .getBoolean("jetty.dump.start");
//        private final File serviceDir;
        private final String[] args;
        
        public HAJournalServerConfiguration(final String serviceName,
                final Configuration config, final UUID serviceId, //
                final int jettyPort,
                /*final File serviceDirIsIgnored, */ final String[] args)
                throws ConfigurationException {

            // Note: ignored! args[] is used instead.
            super(HAJournalServer.ConfigurationOptions.COMPONENT, config);

            if (serviceName == null)
                throw new IllegalArgumentException();

            if (serviceId == null)
                throw new IllegalArgumentException();

            if (serviceDir == null) // ??? This is checking a variable in the base class. is that deliberate?
                throw new IllegalArgumentException();

            if (args == null)
                throw new IllegalArgumentException();

            if (args.length == 0)
                throw new IllegalArgumentException();

            if (args[0] == null) // the configuration file name.
                throw new IllegalArgumentException();

            this.serviceName = serviceName;

            this.serviceId = serviceId;

            this.jettyPort = jettyPort;
            
//            this.serviceDir = serviceDir;
            
            this.args = args;
            
        }

        @Override
        @SuppressWarnings("rawtypes")
        public HAJournalServerStarter newServiceStarter(
                final IServiceListener listener) throws Exception {

            return new HAJournalServerStarter(listener);

        }

        private class HAJournalServerStarter<V extends ProcessHelper> extends
                JavaServiceStarter<V> {

           /**
            * The {@link ServiceItem} iff discovered.
            */
            @SuppressWarnings("unused")
            private ServiceItem serviceItem = null;

            /**
             * Used to override the service directory in the deployed
             * configuration.
             */
            private final String TEST_SERVICE_DIR = "test.serviceDir";
            
            /**
             * Used to override the {@link ServiceID} in the deployed
             * configuration.
             */
            private final String TEST_SERVICE_ID = "test.serviceId";
            
            /**
             * Used to override the logicalServiceId in the deployed
             * configuration.
             */
            private final String TEST_LOGICAL_SERVICE_ID = "test.logicalServiceId";
            
            /**
             * Used to override the port at which jetty sets up the http
             * connection.
             */
            private final String TEST_JETTY_PORT = NanoSparqlServer.SystemProperties.JETTY_PORT;

            /**
             * The path in the local file system to the root of the web
             * application. This is <code>bigdata-war/src</code> in the source
             * code, but the webapp gets deployed to the serviceDir for this
             * test suite.
             */
            private final String JETTY_RESOURCE_BASE = NanoSparqlServer.SystemProperties.JETTY_RESOURCE_BASE;

            private final String JETTY_OVERRIDE_WEB_XML = NanoSparqlServer.SystemProperties.JETTY_OVERRIDE_WEB_XML;

            /**
             * Used to override the <code>jetty.dump.start</code> environment
             * property.
             */
            private final String TEST_JETTY_DUMP_START = NanoSparqlServer.SystemProperties.JETTY_DUMP_START;

            /**
             * The absolute effective path of the service directory. This is
             * overridden on the {@link #TEST_SERVICE_DIR} environment variable
             * and in the deployed HAJournal.config file in order to have the
             * service use the specified service directory when it gets
             * deployed.
             */
            private final String servicePath = HAJournalServerConfiguration.this.serviceDir
                    .getAbsolutePath();

            protected HAJournalServerStarter(final IServiceListener listener) {

                super(listener);
                
            }
            
//            /**
//             * Adds <code>serviceDir</code> into the environment.
//             */
//            @Override
//            protected void setUpEnvironment(final Map<String, String> env) {
//
//                super.setUpEnvironment(env);
//
//                env.put(TEST_SERVICE_DIR, servicePath);
//
//            }
            
            /**
             * Extended to add the configuration file on the command line after
             * the class name.
             */
            @Override
            protected void addCommandArgs(final List<String> cmds) {

                cmds.add("-D" + TEST_SERVICE_DIR + "=" + servicePath);

                cmds.add("-D" + TEST_SERVICE_ID + "=" + serviceId);

                cmds.add("-D" + TEST_LOGICAL_SERVICE_ID + "="
                        + getLogicalServiceId());

                // Override the HTTP port for jetty.
                cmds.add("-D" + TEST_JETTY_PORT + "=" + jettyPort);

                //Per BLZG-1270; now managed in bigdata-jini-test jetty.xml
                // Override the location of the webapp as deployed.
                //cmds.add("-D" + JETTY_RESOURCE_BASE + "=" + WAR_FILE_NAME);

                // Override the location of the override-web.xml file as deployed.
                //cmds.add("-D" + JETTY_OVERRIDE_WEB_XML + "=./WEB-INF/override-web.xml");

                // Override the jetty.dump.start.
                cmds.add("-D" + TEST_JETTY_DUMP_START + "=" + jettyDumpStart);

                super.addCommandArgs(cmds);
                
                for (String arg : args) {

                    // the configuration file, etc.
                    cmds.add(arg);
                    
                }
                
            }

            /**
             * Overridden to monitor for the jini join of the service and the
             * creation of the znode corresponding to the physical service
             * instance.
             * 
             * @todo we could also verify the service using its proxy, e.g., by
             *       testing for a normal run state.
             */
            @Override
            protected void awaitServiceStart(final V processHelper,
                    final long timeout, final TimeUnit unit) throws Exception,
                    TimeoutException, InterruptedException {

//                final long begin = System.nanoTime();

                final long nanos = unit.toNanos(timeout);

                // wait for the service to be discovered
                serviceItem = awaitServiceDiscoveryOrDeath(processHelper,
                        nanos, TimeUnit.NANOSECONDS);

//                // proxy will be used for destroy().
//                processHelper.setServiceItem(serviceItem);

//                // subtract out the time we already waited.
//                final long remaining = nanos - (System.nanoTime() - begin);
//
//                // TODO (restore) wait for the ephemeral znode for the service to be created
//                awaitZNodeCreatedOrDeath(serviceItem, processHelper, nanos,
//                        TimeUnit.NANOSECONDS);

            }

            /**
             * Waits up to timeout units for the service to either by discovered
             * by jini or to die.
             * <p>
             * Note: We recognize the service by the present of the assigned
             * {@link ServiceToken} attribute. If a service with that
             * {@link ServiceToken} can not be discovered by jini after a
             * timeout, then we presume that the service could not start and
             * throw an exception. The {@link ServiceToken} provides an
             * attribute which is assigned by the service starter while the
             * {@link ServiceID} is assigned by jini only after the service has
             * joined with a jini registrar.
             * 
             * @param processHelper
             * @param timeout
             * @param unit
             * @return The {@link ServiceItem} for the discovered service.
             * @throws Exception
             */
            protected ServiceItem awaitServiceDiscoveryOrDeath(
                    final ProcessHelper processHelper, long timeout,
                    final TimeUnit unit) throws Exception, TimeoutException,
                    InterruptedException {

                // convert to ms for jini lookup() waitDur.
                timeout = unit.toMillis(timeout);

                final long begin = System.currentTimeMillis();

                ServiceDiscoveryManager serviceDiscoveryManager = null;
                try {

                    serviceDiscoveryManager = new ServiceDiscoveryManager(
//                            fed.getDiscoveryManagement(),
                            lookupDiscoveryManager,// Note: This is a reference on the TestCase!
                            new LeaseRenewalManager());

                    if (log.isInfoEnabled())
                        log.info("Awaiting service discovery: "
                                + processHelper.name);

                    final ServiceID serviceID = JiniUtil
                            .uuid2ServiceID(serviceId);

                    final ServiceItem[] items = serviceDiscoveryManager.lookup(
                            new ServiceTemplate(//
                                    serviceID, //
                                    null, // iface[]
                                    null // new Entry[]{new Name(serviceName)}
                            ), // template
                            1, // minMatches
                            1, // maxMatches
                            null, // filter
                            timeout//
                            );

                    final long elapsed = System.currentTimeMillis() - begin;

                    if (items.length == 0) {

                        throw new Exception("Service did not start: elapsed="
                                + elapsed + ", name=" + serviceName);

                    }

                    if (items.length != 1) {

                        throw new Exception("Duplicate ServiceTokens? name="
                                + serviceName + ", found="
                                + Arrays.toString(items));

                    }

                    if (log.isInfoEnabled())
                        log.info("Discovered service: elapsed=" + elapsed
                                + ", name=" + processHelper.name + ", item="
                                + items[0]);

                    return items[0];

                } finally {

                    if (serviceDiscoveryManager != null) {

                        serviceDiscoveryManager.terminate();

                    }

                }

            }

//            /**
//             * Waits up to timeout units for the znode for the physical service
//             * to be created or the process to die.
//             * 
//             * @param processHelper
//             * @param timeout
//             * @param unit
//             * 
//             * @throws TimeoutException
//             * @throws InterruptedException
//             * @throws KeeperException
//             */
//            private void awaitZNodeCreatedOrDeath(
//                    final ServiceItem serviceItem,
//                    final ProcessHelper processHelper, final long timeout,
//                    final TimeUnit unit) throws KeeperException,
//                    InterruptedException, TimeoutException {
//
//                // // convert to a standard UUID.
//                // final UUID serviceUUID =
//                // JiniUtil.serviceID2UUID(serviceItem.serviceID);
//
//                // this is the zpath that the service will create.
//                final String physicalServiceZPath = logicalServiceZPath + "/"
//                        + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER + "/"
//                        + serviceUUID;
//
//                // @todo this should pass in the ZooKeeperAccessor.
//                if (!ZNodeCreatedWatcher.awaitCreate(fed.getZookeeper(),
//                        physicalServiceZPath, timeout, unit)) {
//
//                    throw new TimeoutException("zpath does not exist: "
//                            + physicalServiceZPath);
//
//                }
//
//                if (log.isInfoEnabled())
//                    log.info("znode exists: zpath=" + physicalServiceZPath);
//
//                // success.
//                return;
//
//            }

       } // class HAJournalServerStarter
       
    } // class // HAJournalServerConfiguration
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. 
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();//getFileFilter());

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }
    
    @Override
    public void discarded(DiscoveryEvent arg0) {
        // NOP
    }

    @Override
    public void discovered(DiscoveryEvent arg0) {
        // NOP
    }

    /**
     * Wait until we have a met quorum (not necessarily a fully met quorum).
     * 
     * @return The quorum token for that met quorum.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     */
    protected long awaitMetQuorum() throws IOException,
            AsynchronousQuorumCloseException, InterruptedException,
            TimeoutException {

        // Wait for a quorum met.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        return token;

    }

    
    /**
     * Wait until we have a fully met quorum.
     * 
     * @param ticks
     *            A multiplier of the {@link #awaitQuorumTimeout}. This timeout
     *            is used first to await a quorum meet and then to await the
     *            quorum becoming fully met. (Additive, rather than total.)
     * 
     * @return The quorum token for that fully met quorum.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws AsynchronousQuorumCloseException
     */
    protected long awaitFullyMetQuorum(final int ticks) throws IOException,
            AsynchronousQuorumCloseException, InterruptedException,
            TimeoutException {

        // Wait for a quorum met.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout * ticks,
                TimeUnit.MILLISECONDS);

        // Wait for a fully met quorum.
        assertCondition(new Runnable() {
           @Override
            public void run() {
                try {
                    // Verify quorum is FULLY met for that token.
                    assertTrue(quorum.isQuorumFullyMet(token));
                } catch (Exception e) {
                    // Quorum is not fully met.
                    fail("Not Met", e);
                }
            }
        }, awaitQuorumTimeout * ticks,
        TimeUnit.MILLISECONDS);

        return token;

    }

   /**
    * Wait up to 2 ticks for the quorum to be fully met.
    * 
    * @return The token token if the quorum becomes fully met before the timeout.
    */
    protected long awaitFullyMetQuorum() throws IOException,
            AsynchronousQuorumCloseException, InterruptedException,
            TimeoutException {

       return awaitFullyMetQuorum(2); // default 2 ticks
       
    }

    /**
     * Wait until the quorum meets at the successor of the given token.
     * 
     * @param token
     *            A token.
     */
    protected long awaitNextQuorumMeet(final long token) {

        assertCondition(new Runnable() {
           @Override
            public void run() {
                try {
                    final long token2 = quorum.awaitQuorum(100/* ms */,
                            TimeUnit.MILLISECONDS);
                    if (token + 1 == token2) {
                        // Success.
                        return;
                    }
                    // Fail unless we meet at the next token.
                    assertEquals(token + 1, token2);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, getZKSessionTimeout() + 5000, TimeUnit.MILLISECONDS);

        quorum.assertQuorum(token+1);
        
        return token + 1;
        
    }

    /**
     * Verify that the votes for the old consensus timestamp were withdrawn and
     * that the services are now all voting for the current lastCommitTime as
     * reported by the quorum leader (by querying its root blocks).
     * 
     * @param token
     *            The current quorum token.
     * @param oldConsensusVote
     *            The old vote.
     * 
     * @return The timestamp for the new consensus vote
     * 
     * @throws IOException
     * @throws AssertionFailedError
     */
    protected void assertVotesRecast(final long token,
            final long oldConsensusVote) throws IOException {

        // Snapshot of the current votes.
        final Map<Long, UUID[]> votes = quorum.getVotes();

        final UUID[] votesForOldConsensus = votes.get(oldConsensusVote);

        // Nobody should be voting for the old consensus.
        if (votesForOldConsensus != null && votesForOldConsensus.length != 0) {
         
            fail("Votes exist for old consensus: "
                    + Arrays.toString(votesForOldConsensus));
            
        }

        final long lastCommitTime = quorum.getClient().getLeader(token)
                .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
                .getRootBlock().getLastCommitTime();

        // The joined services.
        final UUID[] joined = quorum.getJoined();

        // Quorum is still valid.
        quorum.assertQuorum(token);

        final UUID[] votesForLastCommitTime = votes.get(lastCommitTime);

        assertNotNull(votesForLastCommitTime);

        assertEquals(joined.length, votesForLastCommitTime.length);

    }

    /**
     * Helper class for simultaneous/seqeunced start of 3 HA services.
     */
    protected class ABC {
        
        /**
         * The services.
         */
        final HAGlue serverA, serverB, serverC;

        /**
         * Start of 3 HA services (this happens in the ctor).
         * 
         * @param sequential
         *           True if the startup should be sequential or false
         *           if services should start concurrently.
         * @throws Exception
         */
        public ABC(final boolean sequential)
                throws Exception {

            this(true/* sequential */, true/* newServiceStarts */);

        }

        /**
         * Start of 3 HA services (this happens in the ctor).
         * 
         * @param sequential
         *            True if the startup should be sequential or false if
         *            services should start concurrently.
         * @param newServiceStarts
         *            When <code>true</code> the services are new, the database
         *            should be at <code>commitCounter:=0</code> and the
         *            constructor will check for the implicit create of the
         *            default KB.
         * @throws Exception
         */
        public ABC(final boolean sequential, final boolean newServiceStarts)
                throws Exception {

            if (sequential) {
            
                final HAGlue[] services = startSequenceABC();

                serverA = services[0];

                serverB = services[1];

                serverC = services[2];

            } else {
                
                final List<Callable<HAGlue>> tasks = new LinkedList<Callable<HAGlue>>();

                tasks.add(new StartATask(false/* restart */));
                tasks.add(new StartBTask(false/* restart */));
                tasks.add(new StartCTask(false/* restart */));

                // Start all servers in parallel. Wait up to a timeout.
                final List<Future<HAGlue>> futures = executorService.invokeAll(
                        tasks, 30/* timeout */, TimeUnit.SECONDS);

                serverA = futures.get(0).get();

                serverB = futures.get(1).get();

                serverC = futures.get(2).get();

            }

            // wait for the quorum to fully meet.
            awaitFullyMetQuorum();

            if(newServiceStarts) {
                // wait for the initial commit point (KB create).
                awaitCommitCounter(1L, serverA, serverB, serverC);
            }
            
        }

        public void shutdownAll() throws InterruptedException,
                ExecutionException {

            shutdownAll(false/* now */);
            
        }

        public void shutdownAll(final boolean now) throws InterruptedException,
                ExecutionException {
            
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

            tasks.add(new SafeShutdownATask());
            tasks.add(new SafeShutdownBTask());
            tasks.add(new SafeShutdownCTask());

            // Start all servers in parallel. Wait up to a timeout.
            final List<Future<Void>> futures = executorService.invokeAll(
                    tasks, 30/* timeout */, TimeUnit.SECONDS);

            futures.get(0).get();
            futures.get(1).get();
            futures.get(2).get();

        }

    }

    /**
     * Commits update transaction after awaiting quorum.
     */
    protected void simpleTransaction() throws IOException, Exception {

        // Await quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Figure out which service is the leader.
        final HAGlue leader = quorum.getClient().getLeader(token);

        // Wait until that service is ready to act as the leader.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(leader));

        simpleTransaction_noQuorumCheck(leader);
        
    }

    /**
     * Commits update transaction with LBS after awaiting quorum.
     */
    protected void simpleTransactionLBS() throws IOException, Exception {

        // Await quorum meet.
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);

        // Figure out which service is the leader.
        final HAGlue leader = quorum.getClient().getLeader(token);

        // Wait until that service is ready to act as the leader.
        assertEquals(HAStatusEnum.Leader, awaitNSSAndHAReady(leader));

        simpleTransaction_noQuorumCheckLBS(leader);
        
    }

    /**
     * Immediately issues a simple transaction against the service.
     * 
     * @param leader
     *            The service (must be the leader to succeed).
     *            
     * @throws IOException
     * @throws Exception
     */
    protected void simpleTransaction_noQuorumCheck(final HAGlue leader)
            throws IOException, Exception {

        simpleTransaction_noQuorumCheck(leader, false/* useLoadBalancer */);

    }

    /**
     * Immediately issues a simple transaction against the service with LBS.
     * 
     * @param leader
     *            The service (must be the leader to succeed).
     *            
     * @throws IOException
     * @throws Exception
     */
    protected void simpleTransaction_noQuorumCheckLBS(final HAGlue leader)
            throws IOException, Exception {

        simpleTransaction_noQuorumCheck(leader, true/* useLoadBalancer */);

    }

    /**
     * Immediately issues a simple transaction against the service.
     * 
     * @param haGlue
     *            The service (must be the leader to succeed unless using the
     *            load balancer).
     * @param useLoadBalancer
     *            When <code>true</code> the LBS will be used and the update
     *            request may be directed to any service and will be proxied to
     *            the leader if necessary.
     * @throws IOException
     * @throws Exception
     */
    protected void simpleTransaction_noQuorumCheck(final HAGlue haGlue,
            final boolean useLoadBalancer) throws IOException, Exception {
        
        final StringBuilder sb = new StringBuilder();
        sb.append("DROP ALL;\n");
        sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
        sb.append("INSERT DATA {\n");
        sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
        sb.append("  dc:creator \"A.N.Other\" .\n");
        sb.append("}\n");

        final String updateStr = sb.toString();

        final RemoteRepositoryManager repo = getRemoteRepository(haGlue,
                useLoadBalancer, httpClient);
        try {
           repo.getRepositoryForDefaultNamespace().prepareUpdate(updateStr).evaluate();
        } finally {
        	   repo.close();
        }

    }
    
    /**
     * Verify that an attempt to read on the specified service is disallowed.
     */
    protected void assertWriteRejected(final HAGlue haGlue) throws IOException,
            Exception {

        final StringBuilder sb = new StringBuilder();
        sb.append("DROP ALL;\n");
        sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
        sb.append("INSERT DATA {\n");
        sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
        sb.append("  dc:creator \"A.N.Other\" .\n");
        sb.append("}\n");

        final String updateStr = sb.toString();

         try {
   
            final RemoteRepositoryManager repo = getRemoteRepository(haGlue,
                  httpClient);
            try {
               repo.getRepositoryForDefaultNamespace().prepareUpdate(updateStr).evaluate();
            } finally {
               repo.close();
            }
   
         } catch (HttpException ex) {
   
            log.warn("Status Code: " + ex.getStatusCode(), ex);
   
            assertEquals("statusCode", 405, ex.getStatusCode());
   
         }
        
    }

    /**
     * Verify that an attempt to read on the specified service is disallowed.
     */
    protected void assertReadRejected(final HAGlue haGlue) throws IOException,
            Exception {

        final String queryStr = "SELECT (COUNT(*) as ?count) {?s ?p ?o}";

        try {
           final RemoteRepositoryManager repo = getRemoteRepository(haGlue, httpClient);
        	try {
        		repo.getRepositoryForDefaultNamespace().prepareTupleQuery(queryStr).evaluate();
        	} finally {
        		repo.close();
        	}
            
        } catch (HttpException ex) {
            
            assertEquals("statusCode", 405, ex.getStatusCode());
            
        }
        
    }
    
    protected void assertStoreStates(final HAGlue[] services) throws IOException {
        if (services.length < 2)
            return; // nothing to compare

        final StoreState test = ((HAGlueTest) services[0]).getStoreState();
        final String tname = serviceName(services[0]);

        for (int s = 1; s < services.length; s++) {
            final StoreState other = ((HAGlueTest) services[s]).getStoreState();

            if (!test.equals(other)) {
                final String oname = serviceName(services[s]);
                final String msg = "StoreState mismatch \n" + tname + "\n"
                        + test.toString() + "\n" + oname + "\n"
                        + other.toString();
                fail(msg);
            }
        }
    }
    
    protected String serviceName(final HAGlue s) {
        if (s == serverA) {
            return "serverA";
        } else if (s == serverB) {
            return "serverB";
        } else if (s == serverC) {
            return "serverC";
        } else {
            return "NA";
        }
    }

   /**
    * Task loads a large data set.
    */
   protected class LargeLoadTask implements Callable<Void> {

      private final long token;
      private final boolean reallyLargeLoad;
      private final boolean dropAll;
      private final String namespace;

      @Override
      public String toString() {
         return getClass().getName() + "{token=" + token + ", reallyLargeLoad="
               + reallyLargeLoad + ", dropAll=" + dropAll + ", namespace="
               + namespace + "}";
      }

      /**
       * Large load.
       * 
       * @param token
       *           The token that must remain valid during the operation.
       */
      public LargeLoadTask(final long token) {

         this(token, false/* reallyLargeLoad */);

      }

      public LargeLoadTask(final long token, final boolean reallyLargeLoad) {

         this(token, reallyLargeLoad, true/* dropAll */);

      }

      /**
       * Either large or really large load.
       * 
       * @param token
       *           The token that must remain valid during the operation.
       * @param reallyLargeLoad
       *           if we will also load the 3 degrees of freedom file.
       * @param dropAll
       *           when <code>true</code> a DROP ALL is executed before the
       *           load.
       */
      public LargeLoadTask(final long token, final boolean reallyLargeLoad,
            final boolean dropAll) {

         this(token, reallyLargeLoad, dropAll,
               BigdataSail.Options.DEFAULT_NAMESPACE);

      }

      /**
       * Either large or really large load.
       * 
       * @param token
       *           The token that must remain valid during the operation.
       * @param reallyLargeLoad
       *           if we will also load the 3 degrees of freedom file.
       * @param dropAll
       *           when <code>true</code> a DROP ALL is executed before the
       *           load.
       * @param namespace
       *           The namespace against which the operation will be executed.
       */
      public LargeLoadTask(final long token, final boolean reallyLargeLoad,
            final boolean dropAll, final String namespace) {

         this.token = token;

         this.reallyLargeLoad = reallyLargeLoad;

         this.dropAll = dropAll;

         this.namespace = namespace;

      }

      @Override
      public Void call() throws Exception {

         final StringBuilder sb = new StringBuilder();
         if (dropAll)
            sb.append("DROP ALL;\n");
         sb.append("LOAD <" + getFoafFileUrl("data-0.nq.gz") + ">;\n");
         sb.append("LOAD <" + getFoafFileUrl("data-1.nq.gz") + ">;\n");
         sb.append("LOAD <" + getFoafFileUrl("data-2.nq.gz") + ">;\n");
         if (reallyLargeLoad)
            sb.append("LOAD <" + getFoafFileUrl("data-3.nq.gz") + ">;\n");
         sb.append("INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y };\n");
         sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
         sb.append("INSERT DATA\n");
         sb.append("{\n");
         sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
         sb.append("    dc:creator \"A.N.Other\" .\n");
         sb.append("}\n");

         final String updateStr = sb.toString();

         final HAGlue leader = quorum.getClient().getLeader(token);

         // Verify quorum is still valid.
         quorum.assertQuorum(token);

         final long elapsed;
         final RemoteRepositoryManager mgr = getRemoteRepository(leader, httpClient);
         try {
            final RemoteRepository repo = mgr
                  .getRepositoryForNamespace(namespace);
            final long begin = System.nanoTime();
            if (log.isInfoEnabled())
               log.info("load begin: " + repo);
            repo.prepareUpdate(updateStr).evaluate();
            elapsed = System.nanoTime() - begin;
         } finally {
            mgr.close();
         }

         // Verify quorum is still valid.
         quorum.assertQuorum(token);

         if (log.isInfoEnabled())
            log.info("load done: " + this + ", elapsed="
                  + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");

         // Done.
         return null;

      }

   } // class LargeLoadTask
    
    /**
     * Spin, looking for the quorum to fully meet *before* the LOAD is finished.
     * 
     * @return <code>true</code> iff the LOAD finished before the {@link Future}
     * was done.
     */
    protected boolean awaitFullyMetDuringLOAD(final long token,
            final Future<Void> ft) throws InterruptedException,
            ExecutionException, TimeoutException {

        final long begin = System.currentTimeMillis();
        boolean fullyMetBeforeLoadDone = false;
        while (!fullyMetBeforeLoadDone) {
            final long elapsed = System.currentTimeMillis() - begin;
            if (elapsed > longLoadTimeoutMillis) {
                /**
                 * This timeout is a fail safe for LOAD operations that get HUNG
                 * on the server and prevents CI hangs.
                 */
                throw new TimeoutException(
                        "LOAD did not complete in a timely fashion.");
            }
            try {
                if (quorum.isQuorumFullyMet(token) && !ft.isDone()) {
                    // The quorum is fully met before the load is done.
                    fullyMetBeforeLoadDone = true;
                }
                // Check LOAD for error.
                ft.get(50/* timeout */, TimeUnit.MILLISECONDS);
                // LOAD is done (no errors, future is done).
                assertTrue(fullyMetBeforeLoadDone);
                break;
            } catch (TimeoutException ex) {
                // LOAD still running.
                continue;
            }
        }
        
        return fullyMetBeforeLoadDone;

    }

    /**
     * IMHO a simpler, clearer implementation
     * 
     * @param token
     * @param ft
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    protected boolean awaitFullyMetDuringLOAD2(final long token,
            final Future<Void> ft) throws InterruptedException,
            ExecutionException, TimeoutException {

        try {
            assertTrue(token == awaitFullyMetQuorum((int) (longLoadTimeoutMillis/awaitQuorumTimeout)));
        } catch (AsynchronousQuorumCloseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return !ft.isDone();

    }   
    /**
     * Remove files in the directory, except the "open" log file.
     * 
     * @param dir
     *            The HALog directory.
     * @param openFile
     *            The name of the open log file.
     */
    protected void removeFiles(final File dir, final File openFile) {
        final File[] files = dir.listFiles();
        if (files != null)
            for (File file: files) {
                if (!file.equals(openFile)) {
                    log.warn("removing file " + file.getName());
                    
                    file.delete();
                }
            }
    }

    /**
     * Recursive copy.
     * 
     * @param src
     *            The source (must be a directory).
     * @param dst
     *            The destination (must be a directory, as per source).
     *            
     * @throws IOException
     * 
     * @see #copyFile(File, File, boolean)
     */
    protected void copyFiles(final File src, final File dst) throws IOException {
        if (!src.isDirectory())
            throw new IOException("src not a directory: " + src);
        if (!dst.isDirectory())
            throw new IOException("dst not a directory: " + dst);
        final File[] files = src.listFiles();
        if (files == null)
            return;
        if (log.isInfoEnabled())
            log.info("Copying " + src.getAbsolutePath() + " to "
                    + dst.getAbsolutePath() + ", #=files="
                    + (files == null ? 1 : files.length));
        for (File srcFile : files) {
            final File dstFile = new File(dst, srcFile.getName());
            if (log.isInfoEnabled())
                log.info("Copying " + srcFile.getAbsolutePath() + " to "
                        + dstFile.getAbsolutePath());
            if (srcFile.isDirectory()) {
                if (!dstFile.exists() && !dstFile.mkdirs())
                    throw new IOException("Could not create directory: "
                            + dstFile);
                // Recursive copy.
                copyFiles(srcFile, dstFile);
            } else { 
                // copy a single file.
                copyFile(srcFile, dstFile, false/* append */);
            }
        }
    }

    /**
     * Wait the service self-reports "RunMet".
     */
    protected void awaitRunMet(final HAGlue haGlue) throws Exception {
        // Wait until HA and NSS are ready.
        awaitNSSAndHAReady(haGlue);
        // Wait until self-reports RunMet.
        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {
                    final String extendedRunState = haGlue.getExtendedRunState();
                    if (!extendedRunState.contains("RunMet")) {
                        fail("Expecting RunMet, not " + extendedRunState);
                    }
                } catch (Exception e) {
                    fail();
                }
            }

        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Assert that a snapshot exists for the specific commit point.
     * 
     * @param snapshotDir
     *            The snapshot directory for the service.
     * @param commitCounter
     *            The commit point.
     */
    protected void assertSnapshotExists(final File snapshotDir,
            long commitCounter) {

        assertTrue(CommitCounterUtility.getCommitCounterFile(snapshotDir,
                commitCounter, SnapshotManager.SNAPSHOT_EXT).exists());
        
    }
    
    /**
     * Await the specified snapshot.
     * 
     * @param server
     *            The service.
     * @param commitCounter
     *            The commitCounter for the snapshot.
     */
    protected void awaitSnapshotExists(final HAGlue server,
            final long commitCounter) throws Exception {

        awaitRunMet(server);

        // Wait until self-reports RunMet.
        assertCondition(new Runnable() {
            @Override
            public void run() {
                try {

                    /*
                     * Check the server for an active Future for a snapshot. If
                     * we find one, then just wait for that Future.
                     * 
                     * Note: This WILL NOT schedule a snapshot. It just gets the
                     * current Future (if any).
                     * 
                     * Note: If the snapshot is already done, then this will
                     * return [null]. So we can not tell the difference using
                     * this method between a snapshot that has not been
                     * requested yet and a snapshot that is already done. If the
                     * Future is null we will request the snapshot digest
                     * (below) to figure out if the snapshot already exists.
                     */
                    {
                        final Future<IHASnapshotResponse> ftA = server.takeSnapshot(null/* GET */);

                        if (ftA != null) {

                            final IRootBlockView snapshotRB = ftA.get()
                                    .getRootBlock();

                            if (snapshotRB.getCommitCounter() == commitCounter) {

                                // This is the snapshot that we were waiting
                                // for.
                                return;

                            }

                        }

                    }

                    try {

                        // If we can get the digest then the snapshot exists.
                        server.computeHASnapshotDigest(new HASnapshotDigestRequest(
                                commitCounter));
                        
                        // Found it.
                        return;
                        
                    } catch (Exception ex) {
                        
                        if (!InnerCause.isInnerCause(ex,
                                FileNotFoundException.class)) {

                            log.error("Not expecting: " + ex, ex);
                            
                            fail("Not expecting: " + ex.getMessage(), ex);
                            
                        }
                    }

                } catch (Exception e) {
                    fail();
                }
            }

        }, 10, TimeUnit.SECONDS);

    }

    /**
     * Stop the zookeeper process under test control.
     * 
     * @see #zkCommand(String)
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    protected void stopZookeeper() throws InterruptedException, IOException {
        assertZookeeperRunning();
        log.warn("");
        zkCommand("stop");
        assertZookeeperNotRunning();
    }

    /**
     * Start the zookeeper process under test control.
     * 
     * @see #zkCommand(String)
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    protected void startZookeeper() throws InterruptedException, IOException {
        assertZookeeperNotRunning();
        log.warn("");
        zkCommand("start");
        assertZookeeperRunning();
    }

    /**
     * Execute a command to start or kill zookeeper using the zkServer script.
     * This relies on the system property
     * 
     * <pre>
     * test.zookeeper.installDir
     * </pre>
     * 
     * That property must specify the location of the zookeeper installation.
     * <p>
     * Note: This same property is used by CI to locate the zookeeper install
     * directory and start/stop the zookeeper process. When running these tests,
     * you must specify this property in order to execute tests that stop and
     * start the zookeeper process under test control.
     * <p>
     * Note: The <code>zkServer.sh </code> script DEPENDS on the pid file
     * written by that script. If this gets out of whack, you will not be able
     * to stop zookeeper using this method.
     */
    protected void zkCommand(final String cmd) throws InterruptedException,
            IOException {
        final String pname = "test.zookeeper.installDir";
        String zookeeperDirStr = System.getProperty(pname,"/opt/zookeeper-current");
        if (zookeeperDirStr == null) {
        	//Try for a ZOOKEEPER_HOME
        	zookeeperDirStr = System.getenv("ZOOKEEPER_HOME");
        	if(zookeeperDirStr == null)
             fail("System property not defined: " + pname);
        }
        final File zookeeperDir = new File(zookeeperDirStr);
        if (!zookeeperDir.exists())
            fail("No such file: " + zookeeperDir);
        final File binDir = new File(zookeeperDir, "bin");
        if (!binDir.exists())
            fail("No such file: " + binDir);
        final String shell = SystemUtil.isWindows() ? "cmd" : "/bin/sh";
        final String executable = SystemUtil.isWindows() ? "zkServer.cmd"
                : "zkServer.sh";
        if (log.isInfoEnabled())
            log.info("installDir=" + zookeeperDirStr + ", binDir=" + binDir
                    + ", shell=" + shell + ", executable=" + executable
                    + ", cmd=" + cmd);
        final ProcessBuilder pb = new ProcessBuilder(shell, executable, cmd);
        pb.directory(binDir);
        final int exitCode = pb.start().waitFor();
        /*
         * Make sure that the command executed normally!
         * 
         * Note: exitCode=1 could mean that the pid file is no longer correct
         * hence "stop" can not be processed.
         */
        assertEquals("exitCode=" + exitCode, 0, exitCode);
        // Wait for zk to start or stop.
        Thread.sleep(1000/* ms */);
    }

    /** Verify zookeeper is running on the local host at the client port. */
    protected void assertZookeeperRunning() {

        if (!isZookeeperRunning()) {
            final String pname = "test.zookeeper.installDir";
            final String zookeeperDirStr = System.getProperty(pname,"NOT_DEFINED");
            final File zookeeperDir = new File(zookeeperDirStr);
            final File binDir = new File(zookeeperDir, "bin");
            final String shell = SystemUtil.isWindows() ? "cmd" : "/bin/sh";
            final String executable = SystemUtil.isWindows() ? "zkServer.cmd"
                    : "zkServer.sh";
            fail("Zookeeper not running: localIP=" + getZKInetAddress()
                    + ", clientPort=" + getZKClientPort() + ":: installDir="
                    + zookeeperDirStr + ", binDir=" + binDir + ", shell="
                    + shell + ", executable=" + executable);
        }

    }

    /** Verify zookeeper is not running on the local host at the client port. */
    protected void assertZookeeperNotRunning() {

        if (isZookeeperRunning()) {
            final String pname = "test.zookeeper.installDir";
            final String zookeeperDirStr = System.getProperty(pname,"NOT_DEFINED");
            final File zookeeperDir = new File(zookeeperDirStr);
            final File binDir = new File(zookeeperDir, "bin");
            final String shell = SystemUtil.isWindows() ? "cmd" : "/bin/sh";
            final String executable = SystemUtil.isWindows() ? "zkServer.cmd"
                    : "zkServer.sh";
            fail("Zookeeper is running: localIP=" + getZKInetAddress()
                    + ", clientPort=" + getZKClientPort()+ ":: installDir="
                    + zookeeperDirStr + ", binDir=" + binDir + ", shell="
                    + shell + ", executable=" + executable);
        }
        
    }

    /**
     * Return <code>true</code>iff zookeeper is running on the local host at the
     * client port.
     */
    private boolean isZookeeperRunning() {
        final int clientPort = getZKClientPort();
        final InetAddress localIpAddr = getZKInetAddress();
        boolean running = false;
        try {
            ZooHelper.ruok(localIpAddr, clientPort);
            running = true;
        } catch (Throwable t) {
            log.warn("localIpAddr=" + localIpAddr + ", clientPort="
                    + clientPort + " :: " + t, t);
        }
        return running;
    }

    private int getZKClientPort() {
        return Integer.valueOf(System.getProperty("test.zookeeper.clientPort",
                "2081"));
    }

    private InetAddress getZKInetAddress() {
        return NicUtil.getInetAddress(null, 0, null, true);
    }

}
