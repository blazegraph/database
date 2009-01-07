package com.bigdata.jini.start;

import java.util.UUID;

import org.apache.zookeeper.CreateMode;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;

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
