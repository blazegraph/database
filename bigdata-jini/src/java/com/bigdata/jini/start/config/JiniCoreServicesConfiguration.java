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
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.process.JiniCoreServicesProcessHelper;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.util.JiniServicesHelper;

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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends ServiceConfiguration.Options {
        
        /**
         * The namespace for this component.
         */
        String NAMESPACE = "jini";
        
        /**
         * The location where jini was installed.
         */
        String SERVICE_DIR = ServiceConfiguration.Options.SERVICE_DIR;

        /**
         * The file used to start jini. if not specified, then the appropriate
         * file in the <i>serviceDir</i>/"installverify/support" will be used.
         */
        String CMD = "cmd";
        
    }

    /**
     * The file used to start jini.
     * 
     * @see Options#CMD
     */
    public final File cmd;
    
    /**
     * @param config
     * 
     * @throws ConfigurationException
     */
    public JiniCoreServicesConfiguration(Configuration config)
            throws ConfigurationException {

        super(Options.NAMESPACE, config);
        
        cmd = (File) config.getEntry(Options.NAMESPACE, Options.CMD,
                File.class, null/* default */);

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

        final File supportDir = new File(serviceDir + File.separator
                + "installverify" + File.separator + "support");

        final JiniClientConfig clientConfig;

        protected JiniCoreServicesStarter(final IServiceListener listener,
                final JiniClientConfig clientConfig) {

            super(listener);

            if (clientConfig == null)
                throw new IllegalArgumentException();

            this.clientConfig = clientConfig;
            
            if(!serviceDir.exists()) {
                
                throw new RuntimeException("jini not installed: " + serviceDir);

            }
            
        }

        @SuppressWarnings("unchecked")
        @Override
        protected V newProcessHelper(String className,
                ProcessBuilder processBuilder, IServiceListener listener)
                throws IOException {

            return (V) new JiniCoreServicesProcessHelper(className,
                    processBuilder, listener);

        }
        
        /**
         * Figures out the executable depending on the platform if
         * {@link Options#CMD} was not specified.
         */
        @Override
        protected void addCommand(final List<String> cmds) {

            // the executable.
            
            File cmd = JiniCoreServicesConfiguration.this.cmd;
            
            if (cmd == null) {

                cmd = new File(supportDir, "launch-all.exe");

                if (!cmd.exists()) {

                    cmd = new File(supportDir, "launch-all");
                    
                } else if (!cmd.exists()) {

                    throw new RuntimeException("Could not locate executable: "
                            + supportDir);
                    
                }

            }

            if (log.isInfoEnabled())
                log.info("Will run: " + cmd);
            
            cmds.add(cmd.toString());

        }

        @Override
        protected void setUp() throws Exception {
            
            if (!serviceDir.exists())
                throw new RuntimeException("Directory does not exist: "
                        + serviceDir);

            if (!supportDir.exists())
                throw new RuntimeException("Directory does not exist: "
                        + supportDir);

            super.setUp();

        }
        
        /**
         * Overriden to monitor for the discovery of the service registrar on
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
            final ServiceRegistrar[] registrars = JiniServicesHelper
                    .getServiceRegistrars(1/* maxCount */,
                            clientConfig.groups, /* clientConfig. */locators,
                            nanos, unit);

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
                        + "ms, locators=" + locators);

            }

//            final LookupLocator locator = new LookupLocator("jini://localhost");
//
//            if (!JiniCoreServicesHelper.isJiniRunning(
//                    LookupDiscovery.ALL_GROUPS,
//                    new LookupLocator[] { locator }, timeout, unit)) {
//
//                throw new TimeoutException("Registrar not found: timeout="
//                        + timeout + "ms, locator=" + locator);
//
//            }
//            
//            if(INFO)
//                log.info("Discovered registrar: locator="+locator);

        }

    }

}
