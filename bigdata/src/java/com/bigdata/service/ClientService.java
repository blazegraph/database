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
 * Created on Apr 23, 2009
 */

package com.bigdata.service;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.Banner;

/**
 * A service for distributing application {@link Callable}s across an
 * {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ClientService extends AbstractService implements
        IClientService {

    protected static final Logger log = Logger.getLogger(DataService.class);

    private final Properties properties;

    /**
     * Configuration options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options extends IBigdataClient.Options {
        
    }
    
    /**
     * 
     */
    public ClientService(final Properties properties) {

        // show the copyright banner during statup.
        Banner.banner();

        this.properties = (Properties) properties.clone();
        
    }

    @Override
    public AbstractService start() {
    
        /*
         * Note: There is nothing to do here - the main "service" is the
         * federation's executor service.
         */
        
        return this;

    }

    /**
     * Returns either {@link IClientService}.
     */
    public Class<? extends IClientService> getServiceIface() {

        return IClientService.class;

    }
    
    /**
     * Note: When the {@link ClientService} is accessed via RMI the
     * {@link Future} MUST be a proxy. This gets handled by the concrete server
     * implementation.
     * 
     * @see AbstractDistributedFederation#getProxy(Future)
     * 
     * @todo Map/reduce can be handled in the this manner.
     *       <p>
     *       Note that we have excellent locators for the best data service when
     *       the map/reduce input is the scale-out repository since the task
     *       should run on the data service that hosts the file block(s). When
     *       failover is supported, the task can run on the service instance
     *       with the least load. When the input is a networked file system,
     *       then additional network topology smarts would be required to make
     *       good choices.
     * 
     * @todo we should probably put the federation object in a sandbox in order
     *       to prevent various operations by tasks running in the
     *       {@link DataService} using the {@link IDataServiceCallable}
     *       interface to gain access to the {@link DataService}'s federation.
     *       for example, if they use {@link AbstractFederation#shutdownNow()}
     *       then the {@link DataService} itself would be shutdown.
     */
    public Future<? extends Object> submit(final Callable<? extends Object> task) {

        setupLoggingContext();

        try {

            if (task == null)
                throw new IllegalArgumentException();

            if (task instanceof IFederationCallable) {

                ((IFederationCallable) task).setFederation(getFederation());

            }

            // submit the task and return its Future.
            return getFederation().getExecutorService().submit(task);

        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Extended to attach the various performance counters reported by the
     * {@link DistributedTransactionService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class ClientServiceFederationDelegate extends
            DefaultServiceFederationDelegate<ClientService> {

        public ClientServiceFederationDelegate(final ClientService service) {

            super(service);

        }

//        /**
//         * Note: May be extended to setup service specific counters.
//         * 
//         * @todo write the client URL onto a file in the service's data
//         *       directory.
//         */
//        public void didStart() {
//
//            super.didStart();
//
//            setupCounters();
//
//            // logHttpdURL(dir);
//
//        }
//
//        /**
//         * Sets up {@link AbstractTransactionService} specific counters.
//         */
//        protected void setupCounters() {
//
//            if (getServiceUUID() == null) {
//
//                throw new IllegalStateException(
//                        "The ServiceUUID is not available yet");
//
//            }
//
//            if (!service.isOpen()) {
//
//                /*
//                 * The service has already been closed.
//                 */
//
//                log.warn("Service is not open.");
//
//                return;
//
//            }
//
//            /*
//             * Service specific counters.
//             */
//
//            final CounterSet serviceRoot = service.getFederation()
//                    .getServiceCounterSet();
//
//            serviceRoot.attach(service.getCounters());
//
//        }

    }

}
