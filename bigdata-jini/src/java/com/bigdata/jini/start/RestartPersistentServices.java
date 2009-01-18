package com.bigdata.jini.start;

import java.util.concurrent.Callable;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.service.jini.JiniFederation;

/**
 * Task restarts persistent physical services that should be running on this
 * host but which are not discoverable using jini (not found when we query for
 * their {@link ServiceID}) and apparently disconnected from zookeeper (their
 * ephemeral znode is not found in the {@link BigdataZooDefs#MASTER_ELECTION}
 * container). Service restarts are logged. An error is logged if a service can
 * not be restarted. It is an error if the {@link ZooKeeper} client is not
 * connected. It is an error if no {@link ServiceRegistrar}s are joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RestartPersistentServices implements Callable<Void> {

    protected static final Logger log = Logger.getLogger(RestartPersistentServices.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * A fatal error (jini registrars not joined, zookeeper client is
     * disconnected, etc).
     */
    static transient final String ERR_WILL_NOT_RESTART_SERVICES = "Will not restart services";
    
    protected final JiniFederation fed;
    
    protected final ZooKeeper zookeeper;
    
    public RestartPersistentServices(final JiniFederation fed) {
        
        if(fed == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;
        
        this.zookeeper = fed.getZookeeper();
        
    }
    
    public Void call() throws Exception {

        /*
         * Make sure that the zookeeper client is connected (of course it could
         * disconnect at any time).
         */
        {
            final ZooKeeper.States state = zookeeper.getState();
            switch (state) {
            default:
                log.error(ERR_WILL_NOT_RESTART_SERVICES
                        + " : zookeeper not connected: state=" + state);
                return null;
            case CONNECTED:
                break;
            }
        }

        /*
         * Make sure that we are joined with at least one jini registrar.
         */
        if (fed.getDiscoveryManagement().getRegistrars().length == 0) {
            
            log.error(ERR_WILL_NOT_RESTART_SERVICES
                    + " : not joined with any service registrars.");
            
            return null;
            
        }
        
        // root znode for the federation.
        final String zroot = fed.getZooConfig().zroot;

        // znode for configuration metadata.
        final String zconfig = zroot + BigdataZooDefs.ZSLASH
                + BigdataZooDefs.CONFIG;

        /*
         * FIXME This must scan physical service instances, compare any which
         * were started on the local host with the discovered services, and
         * restart those which are not discoverable using their
         * {@link ServiceConfiguration} znode.
         */
        throw new UnsupportedOperationException();
        
//        return null;
        
    }
    
}
