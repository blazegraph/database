package com.bigdata.jini.start.config;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.ServicesManagerServer;
import com.bigdata.jini.start.process.ProcessHelper;
import com.bigdata.service.jini.IReplicatableService;
import com.bigdata.service.jini.JiniFederation;
import com.sun.jini.config.ConfigUtil;
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
 * configuration znode. [add validation check during (de-)serialization and and
 * also in the public ctor. that will let people set the fields to any values,
 * but only valid data can be written into zookeeper]
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
         * The timeout in milliseconds for an instance of the service to start
         * (the default is dependent on the service type).
         */
        String TIMEOUT = "timeout";
        
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
     */
    public final IServiceConstraint[] constraints;

    /**
     * The timeout in milliseconds for a service instance to start.
     * 
     * @see Options#TIMEOUT
     */
    public final long timeout;
    
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());

        sb.append("{ className=" + className);

        sb.append(", " + Options.ARGS + "=" + Arrays.toString(args));

        sb.append(", " + Options.OPTIONS + "=" + Arrays.toString(options));

        sb.append(", " + Options.SERVICE_DIR + "=" + serviceDir);

        sb.append(", " + Options.TIMEOUT + "=" + timeout);
        
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

        serviceDir = getServiceDir(className, config);

        if (serviceDir == null)
            throw new IllegalArgumentException();

        timeout = getTimeout(className, config);
        
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
       
        constraints = getConstraints(className, config);

    }

    /**
     * The default used for {@link Options#TIMEOUT}.
     */
    protected static long getDefaultTimeout() {
        
        return TimeUnit.SECONDS.toMillis(20);

    }

    protected static long getTimeout(final String className,
            final Configuration config) throws ConfigurationException {

        return (Long) config.getEntry(className, Options.TIMEOUT, Long.TYPE, config
                .getEntry(Options.NAMESPACE, Options.TIMEOUT, Long.TYPE,
                        getDefaultTimeout()));
        
    }

    /**
     * Verify that we could start this service. All constraints that would be
     * violated are logged.
     * <p>
     * Note: Constraints which can be evaluated without the federation reference
     * MUST NOT throw an exception if that reference is <code>null</code>.
     * This allows us to evaluate constraints for boostrap services as well as
     * for {@link ManagedServiceConfiguration}s
     * 
     * @param fed
     *            The federation.
     */
    public boolean canStartService(final JiniFederation fed) {

        boolean canStart = true;
        
        for (IServiceConstraint constraint : constraints) {

            try {

                if (!constraint.allow(fed)) {

                    if (INFO)
                        log.info("Violates: " + constraint);

                    canStart = false;

                }

            } catch (Exception ex) {

                log.error(this, ex);

                return false;

            }
            
        }
        
        if (INFO)
            log.info("canStart=" + canStart+" : "+this);
        
        return canStart;

    }

    /**
     * Factory method returns an object that may be used to start an new
     * instance of the service.
     * 
     * @param listener
     * 
     * @throws Exception
     *             if there is a problem creating the service starter.
     */
    abstract protected AbstractServiceStarter newServiceStarter(
            IServiceListener listener) throws Exception;

    /**
     * A runnable object that will start an instance of a service described by
     * its {@link ServiceConfiguration}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public abstract class AbstractServiceStarter<V extends ProcessHelper>
            implements Callable<V> {
        
        protected final IServiceListener listener;

        /**
         * 
         * @param listener
         */
        protected AbstractServiceStarter(final IServiceListener listener) {

             if (listener == null)
                throw new IllegalArgumentException();
            this.listener = listener;
            
        }

        /**
         * Start a new instance of the service.
         * <p>
         * Note: Output of the child process will be copied onto the output of
         * the parent process. That is where to look for any output that is
         * written onto stdout or stderr. Normally you will want the services to
         * write their logs on a file, syslogd, or an async appender.
         */
        public V call() throws Exception {

            if (INFO)
                log.info("config: " + this);

            // hook for setup before the process starts.
            setUp();

            // create the command line.
            final List<String> cmds = getCommandLine();

            final ProcessBuilder processBuilder = newProcessBuilder(cmds);

            // allow override of the environment for the child.
            setUpEnvironment(processBuilder.environment());

            // specify the startup directory?
            // processBuilder.directory(dataDir);

            // start the process.
            final V processHelper = (V) newProcessHelper(className,
                    processBuilder, listener);

            /*
             * Note: If the services is not started after a timeout then we kill
             * the process. The semantics of "started" is provided by the
             * awaitServiceStart() method and can be overriden depending on the
             * service type.
             */
            Future future = null;
            try {

                /*
                 * Set a thread that will interrupt the [currentThread] if it
                 * notices that the process has died.
                 * 
                 * Note: This provides an upper bound on how long we will wait
                 * to decide that the service has started.
                 */
                future = processHelper.interruptWhenProcessDies(timeout,
                        TimeUnit.MILLISECONDS);

                // attempt to detect a service start failure.
                awaitServiceStart(processHelper, timeout, TimeUnit.MILLISECONDS);

            } catch (InterruptedException ex) {

                /*
                 * If we were interrupted because the process is dead then add
                 * that information to the exception.
                 */
                try {

                    /*
                     * @todo a little wait here appears to be necessary
                     * indicating that there is some problem with
                     * ProcessHelper#interruptWhenProcessDies().
                     */
                    final int exitValue = processHelper.exitValue(10,
                            TimeUnit.MILLISECONDS);

                    throw new IOException("Process is dead: exitValue="
                            + exitValue);

                } catch (TimeoutException ex2) {

                    // ignore.

                }

                // otherwise just rethrow the exception.
                throw ex;

            } catch (Throwable t) {

                /*
                 * The service did not start normally. kill the process and log
                 * an error.
                 */

                try {

                    log.error("Startup problem: " + className, t);

                    throw new RuntimeException(t);

                } finally {

                    processHelper.kill();

                }

            } finally {

                if (future != null) {

                    /*
                     * Note: We MUST cancel the thread monitoring the process
                     * before we leave this scope or it may cause a spurious
                     * interrupt of this thread in some other context!
                     */
                    
                    future.cancel(true/* mayInterruptIfRunning */);
                    
                }

            }

            return (V) processHelper;

        }

        /**
         * Hook for extending the pre-start setup for the service.
         */
        protected void setUp() throws Exception {

            // NOP

        }

        /**
         * Return the {@link ProcessHelper} that will be used to manage the
         * process.
         * 
         * @param className
         * @param processBuilder
         * @param listener
         * 
         * @return
         * 
         * @throws IOException
         */
        protected V newProcessHelper(String className,
                ProcessBuilder processBuilder, IServiceListener listener)
                throws IOException {

            return (V) new ProcessHelper(className, processBuilder, listener);

        }
        
        /**
         * Hook for modification of the child environment.
         * 
         * @param env
         *            A map. Modifications to the map will be written into the
         *            child environment.
         * 
         * @see ProcessBuilder#environment()
         */
        protected void setUpEnvironment(Map<String, String> env) {
            
            // NOP
            
        }
        
        /**
         * Generate the command line that will be used to start the service.
         */
        protected List<String> getCommandLine() {

            final List<String> cmds = new LinkedList<String>();

            addCommand(cmds);

            /**
             * Append JVM args
             */
            addCommandArgs(cmds);

            /**
             * Append any service options.
             */
            addServiceOptions(cmds);

            return cmds;

        }

        /**
         * Create (and possibly configure) a {@link ProcessBuilder} that will be
         * used to start the service.
         * 
         * @param cmds 
         * 
         * @return
         */
        protected ProcessBuilder newProcessBuilder(List<String> cmds) {

            return new ProcessBuilder(cmds);

        }
        
        /**
         * Add the command to be executed (eg, "java", etc).
         * 
         * @param cmds
         */
        abstract protected void addCommand(List<String> cmds);
        
        /**
         * Adds command arguments immediately following the executable name.
         * 
         * @param cmds
         */
        protected void addCommandArgs(List<String> cmds) {

            for (String arg : args) {

                cmds.add(arg);

            }

        }
        
        /**
         * Add options at the end of the command line.
         * 
         * @param cmds
         */
        protected void addServiceOptions(List<String> cmds) {

            for (String arg : options) {

                cmds.add(arg);

            }

        }

        /**
         * Waits a bit to see if the process returns an exit code. If an exit is
         * NOT available after a timeout, then assumes that the process started
         * successfully.
         * <p>
         * Note: <strong>This DOES NOT provide direct confirmation that the
         * service is running in a non-error and available for answering
         * requests.</strong> You SHOULD override this method if you have a
         * service specific means of obtaining such confirmation.
         * 
         * @throws Exception
         *             If a service start failure could be detected (the caller
         *             will kill the process and log an error if any exception
         *             is thrown).
         */
        protected void awaitServiceStart(final V processHelper,
                final long timeout, final TimeUnit unit) throws Exception {

            try {

                final int exitValue = processHelper.exitValue(timeout, unit);

                throw new IOException("exitValue=" + exitValue);

            } catch (TimeoutException ex) {

                /*
                 * Note: Assumes the service started normally!
                 */

                log.warn("Started service: " + className);

                return;

            }

        }

    }
    
    /*
     * Configuration helpers.
     */

    /**
     * Return the directory for the persistent state of the service. This is
     * where it will store its configuration, its serviceUUID (for jini
     * services, once assigned by jini), and any persistent state maintained by
     * the service.
     * 
     * @throws ConfigurationException
     * 
     * @see Options#SERVICE_DIR
     */
    public static File getServiceDir(final String className,
            final Configuration config) throws ConfigurationException {

        File val = (File) config.getEntry(className, Options.SERVICE_DIR,
                File.class, null/* defaultValue */);

        if (val == null) {

            val = (File) config.getEntry(Options.NAMESPACE,
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

    protected static IServiceConstraint[] getConstraints(
            final String className, final Configuration config)
            throws ConfigurationException {

        final IServiceConstraint[] a = (IServiceConstraint[]) config.getEntry(
                className, Options.CONSTRAINTS, IServiceConstraint[].class,
                null /* defaultValue */);

        final IServiceConstraint[] b = (IServiceConstraint[]) config.getEntry(
                Options.NAMESPACE, Options.CONSTRAINTS,
                IServiceConstraint[].class, new IServiceConstraint[0]);

        if (a != null && b != null)
            return concat(a, b);

        if (a != null)
            return a;

        return b;

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
    public static <T> T[] concat(final T[] a, final T[] b) {

        if (a == null && b == null)
            return a;

        if (a == null)
            return b;

        if (b == null)
            return a;

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
     * 
     * @todo Use {@link ConfigUtil#stringLiteral(String)} instead?
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

    /**
     * Splits the CLASSPATH as reported by {@link System#getProperty(String)}
     * into a {@link String}[].
     */
    public static String[] getClassPath() {

        return System.getProperty("java.class.path").split(File.pathSeparator);

    }
    
}
