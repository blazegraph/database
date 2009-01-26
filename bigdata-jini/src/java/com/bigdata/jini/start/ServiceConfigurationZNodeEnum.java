package com.bigdata.jini.start;

import com.bigdata.jini.start.config.ServiceConfiguration;

/**
 * Type-safe enum classifying the different kinds of znodes for a
 * {@link ServiceConfiguration}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ServiceConfigurationZNodeEnum {

    /**
     * A {@link ServiceConfiguration} znode.
     */
    ServiceConfiguration,

    /**
     * A logicalService.
     */
    LogicalService,
    
    /**
     * A container whose children are ephemeral znodes representing physical
     * service instances.
     */
    PhysicalServicesContainer,
    
    /**
     * A container whose children are the physical services competing to become
     * the master for a logical service.
     */
    MasterElection,
    
    /**
     * An ephemeral znode representing one of the physical services competing
     * to become the master for a logical service. 
     */
    MasterElectionLock,
    
    /**
     * An ephemeral znode representing physical service instance.
     */
    PhysicalService;
    
    private ServiceConfigurationZNodeEnum(){}
    
    /**
     * Given the zpath of a {@link ServiceConfiguration} znode and another
     * zpath, return the type of that other zpath.
     * 
     * @param serviceConfigZPath
     *            The zpath of the {@link ServiceConfiguration} znode.
     * @param zpath
     *            The other zpath.
     * 
     * @return The type of the other znode.
     * 
     * @throws RuntimeException
     *             if the other zpath can not be classified.
     */
    public static ServiceConfigurationZNodeEnum getType(final String serviceConfigZPath,
            final String zpath) {

        if (serviceConfigZPath == null)
            throw new IllegalArgumentException();

        if (zpath == null)
            throw new IllegalArgumentException();
        
        final int pos = zpath.lastIndexOf('/');

        final String parent = zpath.substring(0,pos);

        final String child = zpath.substring(pos + 1);

        if (zpath.equals(serviceConfigZPath)) {

            /*
             * This is the ServiceConfiguration znode itself.
             */
            
            return ServiceConfiguration;
            
        }
        
        if (parent.equals(serviceConfigZPath)) {

            /*
             * The parent is the serviceConfiguration znode, so the child is
             * a logicalService.
             */
            
            // the only known children are the logical services.
            assert child.startsWith(BigdataZooDefs.LOGICAL_SERVICE_PREFIX) : "zpath="
                    + zpath;

            return LogicalService;

        }

        if (child.equals(BigdataZooDefs.MASTER_ELECTION)) {

            /*
             * The child is the physicalServices znode. Its children are the
             * actual physical services.
             */

            return MasterElection;

        }

        if (parent.endsWith(BigdataZooDefs.MASTER_ELECTION)) {

            /*
             * The child is an EPHEMERAL znode representing a phsical
             * service instance.
             */

            return MasterElectionLock;

        }
        
        if (child.equals(BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER)) {

            /*
             * The child is the physicalServices znode. Its children are the
             * actual physical services.
             */

            return PhysicalServicesContainer;

        }

        if (parent.endsWith(BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER)) {

            /*
             * The child is an EPHEMERAL znode representing a phsical
             * service instance.
             */

            return PhysicalService;

        }

        throw new RuntimeException("serviceConfigZPath=" + serviceConfigZPath
                + ", zpath=" + zpath);

    }

}
