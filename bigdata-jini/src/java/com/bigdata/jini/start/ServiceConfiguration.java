package com.bigdata.jini.start;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.service.jini.IReplicatableService;
import com.bigdata.service.jini.JiniFederation;
import com.sun.jini.tool.ClassServer;

/**
 * A service configuration specifies the target #of services for each type of
 * service, its target replication count, command line arguments, parameters
 * used to configure new instances of the service, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME fields should all be mutable to make it easy to update the data for a
 * configuration znode.
 */
abstract public class ServiceConfiguration implements Serializable {

    protected static final Logger log = Logger.getLogger(ServiceConfiguration.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Options understood by {@link ServiceConfiguration}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * Namespace for these options.
         */
        String NAMESPACE = ServiceConfiguration.class.getName();

        /**
         * Command line arguments represented as a {@link String}[] that will
         * be included in the command line before any service specific command
         * line arguments when a command is executed to start a new service
         * instance.
         * <p>
         * Note: This option MAY NOT be used for a global default but only for a
         * specific {@link ServiceConfiguration} (there are no possible
         * arguments that make sense for all processes).
         */
        String ARGS = "args";

        /**
         * Service options represented as a {@link String}[] that will be added
         * at the end of the command line when starting a new service instance.
         * <p>
         * Note: This option MAY NOT be used for a global default but only for a
         * specific {@link ServiceConfiguration} (there are no possible
         * arguments that make sense for all processes).
         */
        String OPTIONS = "options";

        /**
         * The default path used when creating the directory for a new service
         * instance. This value may be overridden on a per-service type basis.
         * <p>
         * Note: For logical services that support failover, the concrete
         * service directory is assigned dynamically when a physical service
         * instance is created.
         */
        String SERVICE_DIR = "serviceDir";

        /**
         * The #of logical instances of the services type that should be
         * maintained. The {@link ServicesManagerServer} will attempt to maintain this
         * many instances of the logical service.
         */
        String SERVICE_COUNT = "serviceCount";

        /**
         * The #of physical instances of the service which should be maintained
         * for a given logical instance. If the service is comprised of peers,
         * like zookeeper or jini, then this value MUST be ONE (1) and you wil
         * specify the #of peers as the {@link #SERVICE_COUNT}. If the service
         * supports a failover chain with a master and secondaries then this
         * value may be one or more.
         */
        String REPLICATION_COUNT = "replicationCount";
        
        /**
         * Constraints on where a service may be instantiated.
         * 
         * @see IServiceConstraint
         */
        String CONSTRAINTS = "constraints";
        
        /**
         * A immutable set of property names whose values are not directly
         * copied from a {@link Configuration}.
         * 
         * @todo this is pretty kludgy.
         */
        Set<String> reserved = Collections
                .unmodifiableSet(new HashSet<String>(Arrays
                        .asList(new String[] { ARGS,
                                JavaServiceConfiguration.Options.CLASSPATH,
                                JavaServiceConfiguration.Options.LOG4J,
                                SERVICE_DIR })));

    }
    
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
     * Command line arguments for the executable (placed immediately after the
     * command to be executed).
     * 
     * @see Options#ARGS
     */
    public final String[] args;

    /**
     * Service options (placed at the end of the command line).
     * 
     * @see Options#OPTIONS
     */
    public final String[] options;

    /**
     * The directory for the persistent state of the instances of this service
     * type.
     * 
     * @see Options#SERVICE_DIR
     */
    public final File serviceDir;
    
    /**
     * The target service instance count.
     * 
     * @see Options#SERVICE_COUNT
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
     * 
     * @see Options#REPLICATION_COUNT
     */
    public final int replicationCount;

    /**
     * A set of constraints on where the service may be instantiated. For
     * example, at most N instances of a service on a host, only on hosts with a
     * given IP address pattern, etc.
     * 
     * @see Options#CONSTRAINTS
     * 
     * @todo read from {@link Configuration}. 
     */
    public final IServiceConstraint[] constraints = new IServiceConstraint[] {};
    
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());

        sb.append("{ className=" + className);

        sb.append(", " + Options.ARGS + "=" + Arrays.toString(args));

        sb.append(", " + Options.OPTIONS + "=" + Arrays.toString(options));

        sb.append(", " + Options.SERVICE_DIR + "=" + serviceDir);

        sb.append(", " + Options.SERVICE_COUNT + "=" + serviceCount);

        sb.append(", " + Options.REPLICATION_COUNT + "=" + replicationCount);

        sb.append(", " + Options.CONSTRAINTS + "="
                + Arrays.toString(constraints));

        // add properties from subclasses
        toString(sb);
        
        sb.append("}");

        return sb.toString();

    }
    
    /**
     * May be extended to add more properties to the {@link #toString()}
     * representation.
     * 
     * @param sb
     */
    protected void toString(StringBuilder sb) {
        
    }

    /**
     * 
     * @param className
     *            The name of the server class or a token used for servers that
     *            are not started by directly invoking a JVM, such as "jini".
     * @param config
     *            The {@link Configuration}.
     * 
     * @throws ConfigurationException
     */
    public ServiceConfiguration(final String className,
            final Configuration config) throws ConfigurationException {

        if (className == null)
            throw new IllegalArgumentException();
        
        this.className = className;

        args = getArgs(className, config);

        if (args == null)
            throw new IllegalArgumentException();

        for (String s : args) {

            if (s == null)
                throw new IllegalArgumentException();

        }

        options = getOptions(className, config);

        if (options == null)
            throw new IllegalArgumentException();

        for (String s : options) {

            if (s == null)
                throw new IllegalArgumentException();

        }

        serviceDir = getServicesDir(className, config);

        if (serviceDir == null)
            throw new IllegalArgumentException();

        serviceCount = getServiceCount(className, config);

        if (serviceCount < 0) // @todo LTE ZERO?
            throw new IllegalArgumentException();

        replicationCount = getReplicationCount(className, config);
        
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

    }

    /**
     * Factory method returns an object that may be used to start an new
     * instance of the service for the specified path.
     * 
     * @param fed
     * @param listener
     * @param logicalServiceZPath
     *            The path to the logical service whose instance will be
     *            started.
     * 
     * @throws Exception
     *             if there is a problem creating the service starter.
     */
    abstract public AbstractServiceStarter newServiceStarter(
            JiniFederation fed, IServiceListener listener,
            String logicalServiceZPath) throws Exception;

    /**
     * Return a task that will correct any imbalance between the
     * {@link ServiceConfiguration} and the #of logical services.
     */
    public ManageLogicalServiceTask newLogicalServiceTask(JiniFederation fed,
            IServiceListener listener, String configZPath, List<String> children) {

        return new ManageLogicalServiceTask<ServiceConfiguration>(fed,
                listener, configZPath, children, this);

    }

    /**
     * A runnable object that will start an instance of a service described by
     * its {@link ServiceConfiguration}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract class AbstractServiceStarter<V> implements Callable<V> {
        
        protected final JiniFederation fed;
        protected final IServiceListener listener;
        protected final String logicalServiceZPath;
        protected final ZooKeeper zookeeper;

        /**
         * 
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         *            The zpath to the logical service whose instance will be
         *            started. Note that the zpath to the
         *            {@link CreateMode#EPHEMERAL_SEQUENTIAL} node for the
         *            physical service instance MUST be created by that process
         *            so that the life cycle of the ephemeral node is tied to
         *            the life cycle of the process (or at least to its
         *            {@link ZooKeeper} client).
         */
        protected AbstractServiceStarter(final JiniFederation fed,
                final IServiceListener listener,
                final String logicalServiceZPath) {

            if (fed == null)
                throw new IllegalArgumentException();

            if (listener == null)
                throw new IllegalArgumentException();

            if (logicalServiceZPath == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
            this.listener = listener;
            
            this.logicalServiceZPath = logicalServiceZPath;
            
            this.zookeeper = fed.getZookeeper();
            
        }
        
    }
    
    /*
     * Configuration helpers.
     */

    /**
     * Return the directory for the persistent state of the service will
     * execute. This is where it will store its configuration, its serviceUUID
     * (for jini services, once assigned by jini), and any persistent state
     * maintained by the service.
     * 
     * @throws ConfigurationException
     * 
     * @see Options#SERVICES_DIR
     */
    public static File getServicesDir(final String className,
            final Configuration config) throws ConfigurationException {

        File val = (File) config
                .getEntry(className, Options.SERVICE_DIR,
                        File.class, null/* defaultValue */);

        if (val == null) {

            val = (File) config
                    .getEntry(Options.NAMESPACE,
                    Options.SERVICE_DIR, File.class, null/* defaultValue */);

        }

        return val;
        
    }
    
    public static int getServiceCount(String className, Configuration config)
            throws ConfigurationException {

        return (Integer) config.getEntry(className, Options.SERVICE_COUNT,
                Integer.TYPE, Integer.valueOf(1)/* defaultValue */);

    }

    public static int getReplicationCount(String className, Configuration config)
            throws ConfigurationException {

        return (Integer) config.getEntry(className, Options.REPLICATION_COUNT,
                Integer.TYPE, Integer.valueOf(1)/* default */);

    }

    public static String[] getArgs(String className, Configuration config)
            throws ConfigurationException {

        if (config.getEntry(Options.NAMESPACE, Options.ARGS, String[].class,
                null) != null) {

            throw new ConfigurationException("Not permitted in global scope: "
                    + Options.ARGS);

        }

        return (String[]) config.getEntry(className, Options.ARGS,
                String[].class, new String[] {});

    }

    public static String[] getOptions(String className, Configuration config)
            throws ConfigurationException {

        if (config.getEntry(Options.NAMESPACE, Options.OPTIONS, String[].class,
                null) != null) {

            throw new ConfigurationException("Not permitted in global scope: "
                    + Options.OPTIONS);

        }

        return (String[]) config.getEntry(className, Options.OPTIONS,
                String[].class, new String[] {});

    }

    protected static String[] getStringArray(final String name,
            final String className, final Configuration config,
            final String[] defaultValue) throws ConfigurationException {

        final String[] a = (String[]) config.getEntry(className, name,
                String[].class, null /* defaultValue */);

        final String[] b = (String[]) config.getEntry(Options.NAMESPACE, name,
                String[].class, defaultValue);

        if (a != null && b != null)
            return concat(a, b);

        if (a != null)
            return a;

        return b;

    }

    /**
     * Combines the two arrays, appending the contents of the 2nd array to the
     * contents of the first array.
     * 
     * @param a
     * @param b
     * @return
     */
    @SuppressWarnings("unchecked")
    protected static <T> T[] concat(final T[] a, final T[] b) {

        final T[] c = (T[]) java.lang.reflect.Array.newInstance(a.getClass()
                .getComponentType(), a.length + b.length);

        // final String[] c = new String[a.length + b.length];

        System.arraycopy(a, 0, c, 0, a.length);

        System.arraycopy(b, 0, c, a.length, b.length);

        return c;

    }

    /**
     * Quote a string value.
     * 
     * @param v
     *            The value.
     * 
     * @return The quoted value.
     */
    static public String q(String v) {
        
        final int len = v.length();
        
        final StringBuilder sb = new StringBuilder(len + 10);
        
        sb.append("\"");
        
        for(int i=0; i<len; i++) {
            
            char c = v.charAt(i);
            
            switch(c) {
            
            case '\\':
                sb.append("\\\\");
                break;

            default:
                sb.append(c);
                
            }
            
        }
        
        sb.append("\"");
        
        return sb.toString(); 
        
    }
    
}
