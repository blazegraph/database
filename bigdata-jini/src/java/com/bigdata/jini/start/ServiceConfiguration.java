package com.bigdata.jini.start;

import java.io.Serializable;
import java.util.Arrays;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.service.jini.IReplicatableService;
import com.bigdata.util.NV;
import com.sun.jini.tool.ClassServer;

/**
 * A service configuration specifies the target #of services for each type of
 * service, its target replication count, command line arguments, parameters
 * used to configure new instances of the service, etc.
 * 
 * @todo nice and compact serialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceConfiguration implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 648244470740671354L;

    /**
     * The name of server class (or a token used for servers that are not
     * started directly by invoking a JVM, such as "jini").
     */
    public final String className;

    /**
     * The target service instance count.
     */
    public final int serviceCount;

    /**
     * The target replication count for each service instance (the #of services
     * having the same state and providing failover support for one another).
     * This MUST be ONE (1) unless the service implements
     * {@link IReplicatableService}. Services such as jini or the
     * {@link ClassServer} handle failover either by multiple peers (jini) or by
     * statically replicated state ({@link ClassServer}). Their instances are
     * configured directly, with a replication count of ONE (1).
     */
    public final int replicationCount;

    /**
     * Command line arguments for the executable (placed before the class
     * to be executed for a service started using java).
     */
    public final String[] args;

    /**
     * The initial properties for new instances of the service type (the
     * service type itself is indicated by the znode on which this record is
     * written / from which it was read).
     */
    public final NV[] params;

    /**
     * A set of constraints on where the service may be instantiated. For
     * example, at most N instances of a service on a host, only on hosts with a
     * given IP address pattern, etc.
     * 
     * FIXME read from {@link Configuration}.
     */
    public final IServiceConstraint[] constraints = new IServiceConstraint[] {};
    
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());

        sb.append("{ className=" + className);

        sb.append(", serviceCount=" + serviceCount);

        sb.append(", replicationCount=" + replicationCount);

        sb.append(", args=" + Arrays.toString(args));

        sb.append(", params=" + Arrays.toString(params));

        sb.append(", constraints=" + Arrays.toString(constraints));
        
        sb.append("}");

        return sb.toString();

    }

    /**
     * 
     * @param className
     *            The name of server class (or a token used for servers that are
     *            not started directly by invoking a JVM, such as "jini").
     * @param config
     *            The {@link Configuration}.
     *            
     * @throws ConfigurationException
     */
    public ServiceConfiguration(final String className,
            final Configuration config) throws ConfigurationException {

        this(className,//
                getServiceCount(className, config),//
                getReplicationCount(className,config),//
                getArgs(className,config),//
                getParams(className,config)//
        );

    }

    /**
     * New instance (makes a copy of the caller's data).
     * 
     * @param className
     *            The name of server class (or a token used for servers that are
     *            not started directly by invoking a JVM, such as "jini").
     * @param serviceCount
     *            The service instance count.
     * @param replicationCount
     *            The service replication count.
     * @param args
     *            Command line arguments for the JVM or other executable used to
     *            start the service (placed before the class to be executed for
     *            a JVM).
     * @param params
     *            Parameters for the service.
     */
    public ServiceConfiguration(final String className, final int serviceCount,
            final int replicationCount, final String[] args, final NV[] params) {

        if (serviceCount < 0)
            throw new IllegalArgumentException();

        if (replicationCount < 1)
            throw new IllegalArgumentException();

        if (replicationCount != 1) {

            final Class cls;
            try {

                cls = Class.forName(className);

                if (!(cls.isAssignableFrom(IReplicatableService.class)))
                    throw new IllegalArgumentException();

            } catch (ClassNotFoundException e) {

                // unknown values do not support replication.
                throw new IllegalArgumentException();

            }

        }

        if (args == null)
            throw new IllegalArgumentException();

        if (params == null)
            throw new IllegalArgumentException();

        this.className = className;

        this.serviceCount = serviceCount;

        this.replicationCount = replicationCount;

        for (String s : args) {

            if (s == null)
                throw new IllegalArgumentException();

        }

        // copy to prevent side-effects.
        this.args = args.clone();

        for (NV nv : params) {

            if (nv == null)
                throw new IllegalArgumentException();

            if (nv.getName() == null)
                throw new IllegalArgumentException();

            if (nv.getValue() == null)
                throw new IllegalArgumentException();

        }

        // copy to prevent side-effects.
        this.params = params.clone();

    }

    /*
     * Configuration helpers.
     */

    public static int getServiceCount(String className, Configuration config)
            throws ConfigurationException {

        return (Integer) config.getEntry(className, "serviceCount",
                Integer.TYPE, Integer.valueOf(1)/* defaultValue */);

    }

    public static int getReplicationCount(String className, Configuration config)
            throws ConfigurationException {

        return (Integer) config.getEntry(className, "replicationCount",
                Integer.TYPE, Integer.valueOf(1)/* default */);

    }

    public static String[] getArgs(String className, Configuration config)
            throws ConfigurationException {

        return (String[]) config.getEntry(className, "args", String[].class,
                new String[] {}/* defaultValue */);
    }

    public static NV[] getParams(String className, Configuration config)
            throws ConfigurationException {

        return (NV[]) config.getEntry(className, "params", NV[].class,
                new NV[] {}/* defaultValue */);

    }

}
