package com.bigdata.jini.start;

import java.util.UUID;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

import org.apache.zookeeper.CreateMode;

import com.bigdata.service.AbstractService;
import com.bigdata.service.jini.JiniUtil;

/**
 * Interface declaring constants that are used to name various znodes of
 * interest.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BigdataZooDefs {

    /**
     * The slash character used as zpath component delimiter.
     */
    String ZSLASH = "/";
    
    /**
     * The name of the child of the bigdata federation root zpath whose
     * children are the {@link ServiceConfiguration} znodes.
     */
    String CONFIG = "config";

    /**
     * The zname of the child of the zroot where we put all of our lock nodes.
     */
    String LOCKS = "locks";
    
    /**
     * Relative path to a child of the zroot that is watched by the
     * {@link ServicesManagerService}s. Any time a new lock is created under
     * this znode, running {@link ServicesManagerServer} will contend for that
     * lock if they can satisify the {@link IServiceConstraint}s for the new
     * service. The lock node data itself contains the zpath to the
     * logicalService for which a new physicalService must be created. The
     * {@link ServiceConfiguration} is fetched from that zpath and gives the
     * {@link IServiceConstraint}s that must be satisified.
     */
    String LOCKS_CREATE_PHYSICAL_SERVICE = LOCKS + ZSLASH
            + "createPhysicalService";
    
    /**
     * The prefix for the name of a znode that represents a logical service.
     * This znode is a {@link CreateMode#PERSISTENT_SEQUENTIAL} child of the
     * {@link #CONFIG} znode.
     */
    String LOGICAL_SERVICE = "logicalService";

    /**
     * The prefix for the name of a znode that represents a physical service.
     * The actual znode of a physical service is formed by appending the service
     * {@link UUID}. Jini will assign a {@link ServiceID} when the service is
     * first registered and is available from {@link ServiceItem#serviceID}.
     * The {@link ServiceID} can be converted to a normal {@link UUID} using
     * {@link JiniUtil#serviceID2UUID(ServiceID)}. You can also request the
     * service {@link UUID} using the service proxy.
     * 
     * @see AbstractService#getServiceUUID()
     */
    String PHYSICAL_SERVICE = "physicalService";
    
}
