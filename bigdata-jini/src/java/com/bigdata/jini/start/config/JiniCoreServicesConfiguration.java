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

package com.bigdata.jini.start.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscovery;
import net.jini.discovery.LookupDiscoveryManager;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniCoreServicesProcessHelper;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.util.LookupStarter;
import com.bigdata.service.jini.util.LookupStarter;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceStarter;

/**
 * Somewhat specialized configuration for starting the core jini services
 * (reggie, etc) using "Launch-All" or a similar script.
 * <p>
 * Note: The jini core services must be bootstrapped. There is NO dependency on
 * zookeeper. You can start the jini core services or zookeeper independently.
 * They should both be running before you start bigdata services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see JiniCoreServicesProcessHelper
 * @see JiniCoreServicesStarter
 */
public class JiniCoreServicesConfiguration extends ServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 4601254369003651017L;

    /**
     * Configuration options.
     * 
     * FIXME We need to specify the discovery groups and then modify this to
     * start the lookup service (the only thing that we need) using the correct
     * groups override. See {@link LookupStarter}.
     */
    public interface Options extends ServiceConfiguration.Options {
        
        /**
         * The namespace for this component.
         */
        String NAMESPACE = "jini";

        /**
         * The main configuration file for the jini services. This is the file
         * which has the {@link NonActivatableServiceDescriptor}[] for the
         * services which you wish to start when you start jini.
         */
        String CONFIG_FILE = "configFile";

//        /**
//         * The command used to start jini.
//         */
//        String CMD = "cmd";
        
    }

//    /**
//     * The command used to start jini.
//     * 
//     * @see Options#CMD
//     */
//    public final File cmd;

    /**
     * The main configuration file for the jini services.
     */
    public final File configFile;
    
    /**
     * @param config
     * 
     * @throws ConfigurationException
     */
    public JiniCoreServicesConfiguration(Configuration config)
            throws ConfigurationException {

        super(Options.NAMESPACE, config);
        
        configFile = (File) config.getEntry(Options.NAMESPACE,
                Options.CONFIG_FILE, File.class, null/* default */);

    }

    /**
     * Jini core services should start very quickly (2000 ms)
     */
    @Override
    protected long getDefaultTimeout() {

        return 2000;// ms.
        
    }

    /**
     * Not supported.
     * 
     * @throws UnsupportedOperationException
     * 
     * @see {@link #newServiceStarter(IServiceListener, JiniClientConfig)}
     */
    @Override
    public JiniCoreServicesStarter newServiceStarter(IServiceListener listener)
            throws Exception {

        throw new UnsupportedOperationException();

    }

    /**
     * 
     * @param listener
     * @param clientConfig
     *            The client configuration gives us the groups and locators.
     * @return
     * @throws Exception
     */
    public JiniCoreServicesStarter newServiceStarter(IServiceListener listener,
            JiniClientConfig clientConfig)
            throws Exception {

        return new JiniCoreServicesStarter<JiniCoreServicesProcessHelper>(
                listener, clientConfig);

    }

    /**
     * Used to start the core jini services (reggie, etc).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <V>
     */
    public class JiniCoreServicesStarter<V extends JiniCoreServicesProcessHelper>
            extends AbstractServiceStarter<V> {

//        final File supportDir = new File(serviceDir + File.separator
//                + "installverify" + File.separator + "support");

        final JiniClientConfig clientConfig;

        protected JiniCoreServicesStarter(final IServiceListener listener,
                final JiniClientConfig clientConfig) {

            super(listener);

            if (clientConfig == null)
                throw new IllegalArgumentException();

            this.clientConfig = clientConfig;
            
        }

        @SuppressWarnings("unchecked")
        @Override
        protected V newProcessHelper(String className,
                ProcessBuilder processBuilder, IServiceListener listener)
                throws IOException {

            if(configFile == null) {
                throw new NullPointerException("no entry named '"+Options.CONFIG_FILE
                    +"' in test configuration [under namespace '"+Options.NAMESPACE+"' ]" );
            }
           
            if(!configFile.exists()) {
                
                throw new FileNotFoundException(configFile.toString());

            }

            return (V) new JiniCoreServicesProcessHelper(className,
                    processBuilder, listener);

        }
        
        /**
         * Figures out the executable depending on the platform if
         * {@link Options#CMD} was not specified.
         */
        @Override
        protected void addCommand(final List<String> cmds) {

//            // the executable.
//            
//            File cmd = JiniCoreServicesConfiguration.this.cmd;
//            
//            if (cmd == null) {
//
//                cmd = new File(supportDir, "launch-all.exe");
//
//                if (!cmd.exists()) {
//
//                    cmd = new File(supportDir, "launch-all");
//                    
//                } else if (!cmd.exists()) {
//
//                    throw new RuntimeException("Could not locate executable: "
//                            + supportDir);
//                    
//                }
//
//            }
//
//            if (log.isInfoEnabled())
//                log.info("Will run: " + cmd);
            
            String javaHome = System.getProperty("java.home");
            String javaPath = (javaHome != null ? 
                javaHome + File.separator + "bin" + File.separator + "java" : "java");
            cmds.add(javaPath);

        }

        protected void addCommandArgs(final List<String> cmds) {

            // essentially the JVM args.
            super.addCommandArgs(cmds);
            
            // the class name.
            cmds.add(ServiceStarter.class.getName());
            
            // the config file.
            if(configFile == null) {
                throw new NullPointerException("no entry named '"+Options.CONFIG_FILE
                    +"' in test configuration [under namespace '"+Options.NAMESPACE+"' ]" );
            }
            cmds.add(configFile.toString());
            
        }
        
//        @Override
//        protected void setUp() throws Exception {
//            
//            if (!serviceDir.exists())
//                throw new RuntimeException("Directory does not exist: "
//                        + serviceDir);
//
//            if (!supportDir.exists())
//                throw new RuntimeException("Directory does not exist: "
//                        + supportDir);
//
//            super.setUp();
//
//        }
        
        /**
         * Overridden to monitor for the discovery of the service registrar on
         * the localhost using the groups specified in the
         * {@link JiniClientConfig} and of the {@link LookupLocator}s specified
         * in the {@link JiniClientConfig} which are locators for the local
         * host.
         * 
         * @throws TimeoutException
         *             if a {@link ServiceRegistrar} could not be find within
         *             the timeout for the appropriate groups and locators.
         */
        @Override
        protected void awaitServiceStart(final V processHelper,
                final long timeout, final TimeUnit unit) throws Exception,
                TimeoutException, InterruptedException {

            final long begin = System.nanoTime();
            
            long nanos = unit.toNanos(timeout);
            
            /*
             * The #of registrars that we can locate on this host within a
             * timeout.
             */
            final List<LookupLocator> lst = new LinkedList<LookupLocator>();
            
            for(LookupLocator l : clientConfig.locators) {
                
                if(AbstractHostConstraint.isLocalHost(l.getHost())) {
                    
                    lst.add(l);
                    
                    if(log.isInfoEnabled())
                        log.info("Will verify using locator: "+l);
                    
                }
                
            }
            
            final LookupLocator locators[] = lst.toArray(new LookupLocator[0]);
            
            // adjust for elapsed time.
            nanos -= (System.nanoTime() - begin);

            /*
             * Look for at least one registrar on the local host using the
             * configured locators. We do not wait beyond the timeout.
             */
            final ServiceRegistrar[] registrars = 
                    getServiceRegistrars(1/* maxCount */,
                            clientConfig.groups, /* clientConfig. */locators,
                            nanos, TimeUnit.NANOSECONDS);

            // elapsed time (ns).
            final long elapsed = (System.nanoTime() - begin);
            
            // adjust for elapsed time.
            nanos -= elapsed;

            if (log.isInfoEnabled())
                log
                        .info("Registrars: #found=" + registrars.length
                                + ", elapsed="
                                + TimeUnit.NANOSECONDS.toMillis(elapsed));

            if(registrars.length == 0) {
                
                throw new TimeoutException("Registrar not found: timeout="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed)
                        + "ms, locators="
                        + (locators == null ? null : Arrays.toString(locators)));

            }

        }

    }

    /**
     * Return Jini registrars discovered within the specified timeout.
     * 
     * @param maxCount
     *            The maximum #of registrars to discover.
     * @param groups
     *            An array of groups or {@link LookupDiscovery#ALL_GROUPS} if
     *            you will be using multicast discovery.
     * @param locators
     *            An array of {@link LookupLocator}s. These use URIs of the
     *            form <code>jini://host/</code> or
     *            <code>jini://host:port/</code>. This MAY be an empty array
     *            if you want to use <em>multicast</em> discovery.
     * 
     * @throws IOException
     */
    static public ServiceRegistrar[] getServiceRegistrars(int maxCount,
            final String[] groups, final LookupLocator[] locators,
            long timeout, final TimeUnit unit) throws InterruptedException,
            IOException {
        
        final long begin = System.nanoTime();

        timeout = unit.toNanos(timeout);

        final Object signal = new Object();

        final LookupDiscoveryManager discovery = new LookupDiscoveryManager(groups,
                locators,
                /*
                 * Add a listener that wakes us up if a registrar is discovered.
                 */
                new DiscoveryListener() {

                    public void discarded(DiscoveryEvent e) {

                        if(log.isDebugEnabled())
                            log.debug("discarded: "+e);

                        // ignored.

                    }

                    public void discovered(DiscoveryEvent e) {

                        if(log.isDebugEnabled())
                            log.debug("discovered: "+e);
                        
                        synchronized (signal) {

                            signal.notify();

                        }

                    }
        
        });
                
        try {

            long elapsed;

            // demand some results.
            ServiceRegistrar[] registrars = new ServiceRegistrar[0];

            while ((timeout -= (elapsed = (System.nanoTime() - begin))) > 0
                    && registrars.length < maxCount) {

                synchronized (signal) {

                    try {
                        signal.wait(TimeUnit.NANOSECONDS.toMillis(timeout));
                    } catch(InterruptedException ex) {
                        // fall through
                    }

                    if(log.isDebugEnabled())
                        log.debug("woke up.");

                }

                registrars = discovery.getRegistrars();

            }

            if (log.isInfoEnabled())
                log.info("Found " + registrars.length + " registrars in "
                        + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms.");

            return registrars;

        } finally {

            discovery.terminate();

        }

    }

}
