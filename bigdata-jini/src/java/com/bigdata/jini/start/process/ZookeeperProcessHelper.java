package com.bigdata.jini.start.process;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationFile;

import org.apache.zookeeper.server.PurgeTxnLog;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.ZookeeperServerEntry;
import com.bigdata.jini.start.process.ZookeeperServerConfiguration.ZookeeperRunningException;
import com.bigdata.zookeeper.ZooHelper;

/**
 * Manages the life cycle of a zookeeper instance with extensions to send a
 * "kill" message to the service, periodically send "ruok" messages, etc.
 * 
 * @see QuorumPeerMain
 * @see PurgeTxnLog
 * 
 * @todo periodic [ruok] and alert operator if instance has failed (we could try
 *       to restart, but an operator should really intervene).
 *       <p>
 *       {@link PurgeTxnLog} must be used to periodically release snapshots and
 *       their associated logs which are no longer required for service restart.
 * 
 * @todo periodic purge of snapshots and logs, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperProcessHelper extends ProcessHelper {

    protected final int clientPort;
    
    /**
     * @param name
     * @param builder
     * @param running
     * @param clientPort
     *            The zookeeper client port.
     * @throws IOException
     */
    public ZookeeperProcessHelper(String name, ProcessBuilder builder,
            IServiceListener listener, int clientPort) throws IOException {

        super(name, builder, listener);

        this.clientPort = clientPort;

    }

    /**
     * Extended to send the <code>kill</code> message to the local zookeeper
     * instance.
     * <p>
     * Note: killing zookeeper requires sending a <code>kill</code> command
     * from the host on which it is executing. zookeeper appears to fork a
     * process which is the "real" zookeeper, so just killing the outer process
     * does not do what we want.
     * <p>
     * Note: Due to how zookeeper peers work, we don't really know which
     * instance is answering requests for the clientPort. It could be any
     * instance in the ensemble. Therefore, DO NOT run multiple zookeeper
     * instances on the same host with this class! It will kill the current
     * master!
     * 
     * @throws InterruptedException
     */
    public int kill() throws InterruptedException {

        try {

            /*
             * Send a "kill" message to the zookeeper instance on the local
             * host.
             */

            ZooHelper.kill(clientPort);

        } catch (IOException e) {

            log.error(e.getLocalizedMessage(), e);
            
            return super.kill();

        }

        try {

            /*
             * Wait for zookeeper to die, but only up to a timeout.
             */
            
            final int exitValue = exitValue(5, TimeUnit.SECONDS);
            
            listener.remove(this);
            
            return exitValue;
            
        } catch (TimeoutException e) {
            
            /*
             * Since the process did not die we probably killed the wrong instance.
             * 
             * @todo this tries to kill it using the super class to kill the
             * process. This could cause two instances of zookeeper (on the
             * localhost) to be killed since we may have already killed the
             * wrong one above. An alternative here would be to wrap and rethrow
             * the TimeoutException.
             */
            
            log.error(this, e);
            
            return super.kill();
            
        }
        
    }

    /**
     * (Re-)starts any zookeeper server(s) for the localhost that are identified
     * in the {@link ConfigurationFile} and are not currently running. 
     * 
     * @return the #of instances (re-)started.
     * 
     * @throws ConfigurationException
     * @throws IOException
     * 
     * @see ZookeeperServerConfiguration#newServiceStarter(IServiceListener, ZookeeperServerEntry)
     */
    static public int startZookeeper(final Configuration config,
            final IServiceListener listener) throws ConfigurationException,
            IOException {

        final ZookeeperServerConfiguration serverConfig = new ZookeeperServerConfiguration(
                config);
        
        final ZookeeperServerEntry[] entries = serverConfig
                .getZookeeperServerEntries();

        int nstart = 0;

        for (ZookeeperServerEntry entry : entries) {

            if (entry.isLocalHost()) {

                if (INFO)
                    log.info("Zookeeper instance is local: " + entry);

                try {

                    if (INFO)
                        log.info("Will try to start: " + entry);

                    serverConfig.newServiceStarter(listener, entry).call();

                    nstart++;

                } catch (ZookeeperRunningException ex) {
                    
                    if(INFO)
                        log.info("Already running: "+entry);
                    
                } catch (Exception ex) {

                    log.error("Could not start: entry=" + entry, ex);

                }

            }

        } // next entry

        if (INFO)
            log.info("started=" + nstart + " instances");

        return nstart;

    }

}
