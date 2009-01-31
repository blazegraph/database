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
 * Created on Jan 10, 2009
 */

package com.bigdata.service.jini;

import org.apache.log4j.Logger;

import net.jini.config.ConfigurationException;
import net.jini.core.lookup.ServiceItem;

import com.bigdata.jini.start.IServicesManagerService;

/**
 * Utility will broadcast the {@link IServicesManagerService#sighup()} method to
 * all discovered {@link IServicesManagerService}s in federation to which it
 * connects. Each discovered {@link IServicesManagerService} will push the
 * service configuration to zookeeper and then restart any processes for which
 * it has responsibility which are not currently running.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BroadcastSighup {
    
    protected static final Logger log = Logger.getLogger(BroadcastSighup.class);

    protected static final String COMPONENT = BroadcastSighup.class.getName(); 
    
    /**
     * Sends the signal to all discovered {@link IServicesManagerService}s.
     * <p>
     * Configuration options use {@link #COMPONENT} as their namespace. The
     * following options are defined:
     * <dl>
     * 
     * <dt>discoveryDelay</dt>
     * <dd>The time in milliseconds to wait for service discovery before
     * proceeding.</dd>
     * 
     * <dt>pushConfig</dt>
     * <dd> If you want to do a service configuration push. </dd>
     * 
     * <dt>restartServices</dt>
     * <dd> If you want the services manager that receives the message to
     * restart any services for which it is responsible which are not currently
     * running. </dd>
     * 
     * </dl>
     * 
     * @param args
     *            Configuration file and optional overrides.
     * 
     * @throws InterruptedException
     * @throws ConfigurationException
     */
    public static void main(final String[] args) throws InterruptedException,
            ConfigurationException {

        final JiniFederation fed = JiniClient.newInstance(args).connect();

        final long discoveryDelay = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, "discoveryDelay", Long.TYPE, 5000L/* default */);

        final boolean pushConfig = (Boolean) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, "pushConfig", Boolean.TYPE, true/* default */);

        final boolean restartServices = (Boolean) fed.getClient()
                .getConfiguration().getEntry(COMPONENT, "restartServices",
                        Boolean.TYPE, true/* default */);

        System.out.println("Waiting " + discoveryDelay
                + "ms for service discovery.");

        Thread.sleep(discoveryDelay/* ms */);

        final ServiceItem[] a = fed.getServicesManagerClient()
                .getServiceCache()
                .getServiceItems(0/* maxCount */, null/* filter */);

        int n = 0;
        for (ServiceItem item : a) {
            
            try {
                
                ((IServicesManagerService) item.service).sighup(pushConfig,
                        restartServices);
                
                n++;
                
            } catch(Throwable t) {
                
                log.warn(item, t);

            }

        }
        
        System.out.println("Signal sent to " + n + " of " + a.length
                + " services managers.");

        System.exit(0);

    }

}
