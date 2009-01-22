/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 4, 2009
 */

package com.bigdata.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.jini.start.MockListener;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.ZookeeperServerConfiguration;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.jini.start.process.ZookeeperProcessHelper;
import com.bigdata.resources.ResourceFileFilter;

/**
 * Abstract base class for zookeeper integration tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractZooTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractZooTestCase() {
        super();
    }

    public AbstractZooTestCase(String name) {
        super(name);
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
     * A configuration file used by some of the unit tests in this package. It
     * contains a description of the zookeeper server instance in case we need
     * to start one.
     */
    protected final String configFile = "file:src/test/com/bigdata/zookeeper/testzoo.config";

    final int sessionTimeout = 2000;// ms

    protected ZooKeeper zookeeper;

    /**
     * ACL used by the unit tests.
     */
    protected final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
    
    protected final MockListener listener = new MockListener();

    private File dataDir = null;
    
//    int clientPort;
    
    public void setUp() throws Exception {

        log.info(getName());
        
        // find ports that are not in use.
        final int clientPort = getPort(2181/* suggestedPort */);
        final int peerPort = getPort(2888/* suggestedPort */);
        final int leaderPort = getPort(3888/* suggestedPort */);
        final String servers = "1=localhost:" + peerPort + ":" + leaderPort;

        // create a temporary file for zookeeper's state.
        dataDir = File.createTempFile("test", ".zoo");
        // delete the file so that it can be re-created as a directory.
        dataDir.delete();
        // recreate the file as a directory.
        dataDir.mkdirs();
        
        final String[] args = new String[] {
        // The configuration file (overrides follow).
                configFile,
                // overrides the clientPort to be unique.
                QuorumPeerMain.class.getName() + "."
                        + ZookeeperServerConfiguration.Options.CLIENT_PORT + "="
                        + +clientPort,
                // overrides servers declaration.
                QuorumPeerMain.class.getName() + "."
                        + ZookeeperServerConfiguration.Options.SERVERS + "=\""
                        + servers + "\"",
                // overrides the dataDir
                QuorumPeerMain.class.getName() + "."
                        + ZookeeperServerConfiguration.Options.DATA_DIR
                        + "=new java.io.File("
                        + ServiceConfiguration.q(dataDir.toString()) + ")"//
        };
        
        System.err.println("args=" + Arrays.toString(args));

        final Configuration config = ConfigurationProvider.getInstance(args);

        // if necessary, start zookeeper (a server instance).
        ZookeeperProcessHelper.startZookeeper(config, listener);

        zookeeper = new ZooKeeper("localhost:" + clientPort, sessionTimeout,

                new Watcher() {

                    public void process(WatchedEvent event) {

                        log.info(event.toString());

                    }
                });
     
        try {
            
            /*
             * Since all unit tests use children of this node we must make sure
             * that it exists.
             */
            zookeeper
                    .create("/test", new byte[] {}, acl, CreateMode.PERSISTENT);

        } catch (NodeExistsException ex) {

            log.info("/test already exits.");

        }

    }

    public void tearDown() throws Exception {

        log.info(getName());
        
        if (zookeeper != null) {

            zookeeper.close();

        }
        
        for(ProcessHelper h : listener.running) {
            
            // destroy zookeeper service iff we started it.
            h.kill(true/*immediateShutdown*/);

        }

        if (dataDir != null) {

            // clean out the zookeeper data dir.
            recursiveDelete(dataDir);

        }
        
    }
    
    /**
     * Class used to test concurrency primitives.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract protected class ClientThread extends Thread {

        private final Thread main;
        
        protected final ReentrantLock lock;

        /**
         * 
         * @param main
         *            The thread in which the test is running.
         * @param lock
         *            A lock.
         */
        public ClientThread(final Thread main, final ReentrantLock lock) {

            if (main == null)
                throw new IllegalArgumentException();

            if (lock == null)
                throw new IllegalArgumentException();

            this.main = main;

            this.lock = lock;

            setDaemon(true);

        }

        public void run() {

            try { 

                run2();

            } catch (Throwable t) {

                // log error since won't be seen otherwise.
                log.error(t.getLocalizedMessage(), t);

                // interrupt the main thread.
                main.interrupt();

            }

        }

        abstract void run2() throws Exception;

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
    private void recursiveDelete(File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * Recursive delete of znodes.
     * 
     * @param zpath
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void destroyZNodes(final String zpath) throws KeeperException,
            InterruptedException {

        // System.err.println("enter : " + zpath);

        final List<String> children = zookeeper.getChildren(zpath, false);

        for (String child : children) {

            destroyZNodes(zpath + "/" + child);

        }

        log.info("delete: " + zpath);

        zookeeper.delete(zpath, -1/* version */);

    }

}
