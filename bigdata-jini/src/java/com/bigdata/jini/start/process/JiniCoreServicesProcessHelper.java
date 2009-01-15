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
 * Created on Jan 11, 2009
 */

package com.bigdata.jini.start.process;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;

import com.bigdata.jini.start.IServiceListener;
import com.bigdata.jini.start.config.HostAllowConstraint;
import com.bigdata.jini.start.config.JiniCoreServicesConfiguration;
import com.bigdata.jini.start.config.JiniCoreServicesConfiguration.JiniCoreServicesStarter;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.JiniServicesHelper;

/**
 * Class for starting the jini services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniCoreServicesProcessHelper extends ProcessHelper {

    /**
     * @param name
     * @param builder
     * @param listener
     * @throws IOException
     */
    public JiniCoreServicesProcessHelper(String name, ProcessBuilder builder,
            IServiceListener listener) throws IOException {
        
        super(name, builder, listener);
        
    }

    /**
     * Start the jini core services if they are configured as either running on
     * this or using multicast (so they could be running on any host) and if
     * there are not enough {@link ServiceRegistrar}s already running.
     * 
     * @param config
     * @param listener
     * 
     * @return <code>true</code> if an instance was started.
     * 
     * @throws Exception
     */
    public static boolean startCoreServices(final Configuration config,
            final IServiceListener listener) throws Exception {

        final JiniCoreServicesConfiguration serviceConfig = new JiniCoreServicesConfiguration(
                config);

        final JiniClientConfig clientConfig = new JiniClientConfig(
                null/* class */, config);

        final LookupLocator[] locators = clientConfig.locators;
        
        if (locators.length > 0) {

            /*
             * Unicast locators were specified.
             * 
             * If this host is any of the named hosts then we will consider a
             * service start.
             * 
             * Note: If no locators were specified then we are using multicast
             * and will always consider a service start.
             */
            
            final String[] allowed = new String[locators.length];
            
            int i = 0;
            for (LookupLocator locator : locators) {

                allowed[i++] = locator.getHost();

            }

            if (!new HostAllowConstraint(allowed).allow()) {

                if (INFO)
                    log.info("Host not selected by locator(s).");
                
                return false;
                
            }

        }

        if (!serviceConfig.canStartService(null/* fed */)) {

            // will not start this service.

            if (INFO)
                log.info("Constraint(s) do not allow service start: " + config);

            return false;
            
        }

        /*
         * The #of registrars that we can locate within a timeout.
         */
        final ServiceRegistrar[] registrars = JiniServicesHelper
                .getServiceRegistrars(Integer.MAX_VALUE/* maxCount */,
                        clientConfig.groups, clientConfig.locators, 1500,
                        TimeUnit.MILLISECONDS);

        if (INFO)
            log.info("registrars: #found=" + registrars.length + ", #desired="
                    + serviceConfig.serviceCount);

        if (registrars.length >= serviceConfig.serviceCount) {

            if (INFO)
                log.info("Enough instances - will not start.");

            return false;

        }

        /*
         * Start a new instance.
         */

        if (INFO)
            log.info("Will start instance: " + InetAddress.getLocalHost()
                    + ", config=" + config);

        final JiniCoreServicesStarter<JiniCoreServicesProcessHelper> serviceStarter = serviceConfig
                .newServiceStarter(listener);

        // start jini.
        final JiniCoreServicesProcessHelper processHelper = serviceStarter
                .call();

        return true;

    }

    /**
     * Not supported.
     * 
     * FIXME I have not worked out yet how to destroy the jini core services. In
     * fact, there are generally 6 services started (reggie is only one). By
     * default they are started in the same activation group. [Note that using
     * {@link Process#destroy()} DOES NOT work.]
     * 
     * @see http://www.dancres.org/cottage/doc/api/com/sun/jini/reggie/package-summary.html
     */
    public int kill() {
       
        log.error("Can not kill jini");
        
        return 1;
        //throw new UnsupportedOperationException();
        
    }
    
}
