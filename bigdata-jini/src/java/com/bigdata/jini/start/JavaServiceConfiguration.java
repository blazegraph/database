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

package com.bigdata.jini.start;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.data.ACL;

import com.bigdata.service.jini.JiniFederation;

/**
 * A service that is implemented in java and started directly using java. The
 * value of the "jvmargs" property in the <code>com.bigdata.jini.start</code>
 * component will be combined with the "args" property for the specific service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class JavaServiceConfiguration extends ServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 3688535928764283524L;

    /**
     * Additional options understood by the {@link JavaServiceConfiguration}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends ServiceConfiguration.Options {

        /**
         * The path to the java executable (optional). This may be used to
         * specify a specific JVM on either a global or a per-service type
         * basis.
         * <p>
         * Note: {@link ACL}s may be used to restrict who can set this
         * property.
         */
        String JAVA = "java";

        /**
         * Command line arguments represented as a {@link String}[] that will
         * be interpreted as arguments to the JVM when starting a new service
         * instance using a {@link JavaServiceConfiguration}.
         * <p>
         * This options is only permitted as a global default (e.g., "-server"
         * or "-ea" make sense for all JVMs).
         */
        String DEFAULT_JAVA_ARGS = "defaultJavaArgs";

        /**
         * CLASSPATH components represented as a {@link String}[] that will be
         * included in any {@link JavaServiceConfiguration} before any service
         * specific classpath components.
         * <p>
         * Note: When NOT specified the classpath of the current JVM will be
         * used. If you specify ANY value for this property, then the classpath
         * of the JVM WILL NOT be passed onto to the child process.
         */
        String CLASSPATH = "classpath";

        /**
         * The default log4j configuration for {@link JavaServiceConfiguration}
         * service instances. This may be overriden on a per-service type basis.
         * It is required for {@link BigdataServiceConfiguration}s.
         */
        String LOG4J = "log4j";

    }

    /**
     * The java executable.
     * 
     * @see Options#JAVA
     */
    public final String java;

    /**
     * Default JVM command line arguments.
     * 
     * @see Options#DEFAULT_JAVA_ARGS
     */
    public final String[] defaultJavaArgs;

    /**
     * The log4j URI (if specified and otherwise <code>null</code>).
     * 
     * @see Options#LOG4J
     */
    public final String log4j;

    /**
     * Optional classpath override and otherwise <code>null</code>.
     * 
     * @see Options#CLASSPATH
     */
    public final String[] classpath;

    protected void toString(StringBuilder sb) {

        super.toString(sb);

        sb.append(", " + Options.JAVA + "=" + java);

        sb.append(", " + Options.DEFAULT_JAVA_ARGS + "="
                + Arrays.toString(defaultJavaArgs));

        sb.append(", " + Options.LOG4J + "=" + log4j);

        sb.append(", " + Options.CLASSPATH + "=" + Arrays.toString(classpath));

    }

    /**
     * @param cls
     * @param config
     * @throws ConfigurationException
     */
    public JavaServiceConfiguration(final Class cls, final Configuration config)
            throws ConfigurationException {

        super(cls.getName(), config);

        this.java = getJava(cls.getName(), config);

        this.defaultJavaArgs = getDefaultJavaArgs(cls.getName(), config);

        this.log4j = getLog4j(cls.getName(), config);

        this.classpath = getClasspath(cls.getName(), config);

    }

    // public AbstractServiceStarter newServiceStarter(
    // ServicesManager servicesManager, String zpath) throws Exception {
    //
    // return new JavaServiceStarter(servicesManager, zpath);
    //        
    // }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    protected class JavaServiceStarter<V extends ProcessHelper> extends
            AbstractServiceStarter<V> {

        /**
         * The class for the service that we are going to start.
         */
        protected final Class cls;

        /**
         * The znode for the logical service (the last component of the
         * {@link AbstractServiceStarter#logicalServiceZPath}.
         */
        protected final String logicalServiceZNode;

        /**
         * A unique token assigned to the service. This is used to recognize the
         * service when it joins with a jini registrar, which is how we know
         * that the service has started successfully and how we learn the
         * physicalServiceZPath which is only available once it is created by
         * the service instance.
         */
        protected final UUID serviceToken;

        /**
         * The canonical service name. This is formed in much the same manner as
         * the {@link #serviceDir} using the service type, the
         * {@link #logicalServiceZNode}, and the unique {@link #serviceToken}.
         * While the {@link #serviceToken} alone is unique, the path structure
         * of the service name make is possible to readily classify a physical
         * service by its type and logical instance.
         */
        protected final String serviceName;

        /**
         * The service instance directory. This is where we put any
         * configuration files and should be the default location for the
         * persistent state associated with the service (services may of course
         * be configured to put aspects of their state in a different location,
         * such as the zookeeper log files).
         * <p>
         * The serviceDir is created using path components from the service
         * type, the {@link #logicalServiceZNode}, and finally the unique
         * {@link #serviceToken} assigned to the service.
         */
        protected final File serviceDir;

        /**
         * @param fed
         * @param listener
         * @param logicalServiceZPath
         */
        protected JavaServiceStarter(final JiniFederation fed,
                final IServiceListener listener,
                final String logicalServiceZPath) {

            super(fed, listener, logicalServiceZPath);

            try {

                cls = Class.forName(className);

            } catch (Exception t) {

                throw new RuntimeException("className=" + className, t);

            }

            // just the child name for the logical service.
            logicalServiceZNode = logicalServiceZPath
                    .substring(logicalServiceZPath.lastIndexOf('/') + 1);

            // unique token used to recognize the service when it starts.
            serviceToken = UUID.randomUUID();

            // The canonical service name.
            serviceName = cls.getSimpleName() + "/" + logicalServiceZNode + "/"
                    + serviceToken;

            // The actual service directory (choosen at runtime).
            serviceDir = new File(new File(new File(
                    JavaServiceConfiguration.this.serviceDir, cls
                            .getSimpleName()), logicalServiceZNode),
                    serviceToken.toString());

        }

        /**
         * Start a new instance of the service. Output of the child process will
         * be copied onto the output of the parent process, so that is where to
         * look for any log4j output that is written onto stdout or stderr.
         * 
         * @param servicesManager
         * @param logicalServiceZPath
         *            The path to the znode for the logical service an instance
         *            of which will be started (must exist).
         */
        public V call() throws Exception {

            if (INFO)
                log.info("config: " + JavaServiceConfiguration.this);

            if (zookeeper.exists(logicalServiceZPath, false/* watch */) == null) {

                throw new IllegalStateException(
                        "Logical service zpath not found: "
                                + logicalServiceZPath);

            }

            // hook for setup before the process starts.
            setUp();

            // create the command line.
            final List<String> cmds = getCommandLine();

            final ProcessBuilder processBuilder = new ProcessBuilder(cmds);

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
                 * 
                 * @todo config timeout
                 */
                final long timeout = 60L;
                final TimeUnit unit = TimeUnit.SECONDS;
                future = processHelper.interruptWhenProcessDies(timeout, unit);

                // attempt to detect a service start failure.
                awaitServiceStart(processHelper, timeout, unit);

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

                    processHelper.destroy();

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

            if (classpath == null) {

                // pass on our classpath to the child.
                env.put("CLASSPATH", System.getProperty("java.class.path"));

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

        /**
         * Generate the command line that will be used to start the service.
         */
        protected List<String> getCommandLine() {

            final List<String> cmds = new LinkedList<String>();

            cmds.add(java != null ? java : "java");

            /*
             * Optional properties to be specified to java on the command line,
             * e.g., the heap size, etc.
             */
            for (String arg : defaultJavaArgs) {

                cmds.add(arg);

            }
            for (String arg : args) {

                cmds.add(arg);

            }

            /*
             * Optional classpath
             * 
             * Note: This OVERRIDES classpath of the current JVM when it is
             * specified.
             */
            if (classpath != null) {

                /*
                 * When [classpath] is specified, we explicitly set that command
                 * line argument.
                 */

                cmds.add("-cp");

                final StringBuilder sb = new StringBuilder();

                for (String e : classpath) {

                    if (sb.length() > 0)
                        sb.append(File.pathSeparatorChar);

                    sb.append(e);

                }

                cmds.add(sb.toString());

            }

            if (log4j != null) {

                cmds.add("-Dlog4j.configuration=\"" + log4j + "\"");

            }

            // the class to be executed.
            cmds.add(className);

            /**
             * Append any service options.
             */
            addServiceOptions(cmds);

            return cmds;

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
         * Hook for extending the pre-start setup for the service.
         * <p>
         * This implementation ensures that the {@link #serviceDir} exists.
         */
        protected void setUp() throws Exception {

            if (!serviceDir.exists()) {

                serviceDir.mkdirs();

            }

        }

    }

    public static String getJava(final String className,
            final Configuration config) throws ConfigurationException {

        String java = (String) config.getEntry(className, Options.JAVA,
                String.class, null/* defaultValue */);

        if (java == null) {

            java = (String) config.getEntry(Options.NAMESPACE, Options.JAVA,
                    String.class, "java");

        }

        return java;

    }

    /**
     * Return the log4j configuration URI for this service type.
     * 
     * @param className
     *            Identifies the service type.
     * @param config
     * 
     * @return The service specific log4j configuration URI or the global log4j
     *         configuration URI if none was specified for this service and
     *         <code>null</code> if no global entry was specified.
     * 
     * @throws ConfigurationException
     * 
     * @see Options#LOG4J
     */
    public static String getLog4j(final String className,
            final Configuration config) throws ConfigurationException {

        String log4j = (String) config.getEntry(className, Options.LOG4J,
                String.class, null/* defaultValue */);

        if (log4j == null) {

            log4j = (String) config.getEntry(Options.NAMESPACE, Options.LOG4J,
                    String.class, null/* defaultValue */);

        }

        return log4j;

    }

    public static String[] getDefaultJavaArgs(String className,
            Configuration config) throws ConfigurationException {

        if (config.getEntry(className, Options.DEFAULT_JAVA_ARGS,
                String[].class, null) != null) {

            throw new ConfigurationException("Only permitted in global scope: "
                    + Options.DEFAULT_JAVA_ARGS);

        }

        return (String[]) config.getEntry(Options.NAMESPACE,
                Options.DEFAULT_JAVA_ARGS, String[].class, new String[] {});

    }

    public static String[] getClasspath(String className, Configuration config)
            throws ConfigurationException {

        return getStringArray(Options.CLASSPATH, className, config, null/* defaultValue */);

    }

}
