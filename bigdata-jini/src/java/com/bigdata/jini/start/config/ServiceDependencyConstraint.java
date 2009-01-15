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
 * Created on Jan 15, 2009
 */

package com.bigdata.jini.start.config;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITransactionService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.MetadataServer;
import com.bigdata.service.jini.TransactionServer;


/**
 * Constraint that another a service must be running before this one can be
 * started.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ServiceDependencyConstraint implements IServiceConstraint {

    protected static final Logger log = Logger
            .getLogger(ServiceDependencyConstraint.class);

    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * JINI must be running.
     */
    public static transient final IServiceConstraint JINI = new JiniRunningConstraint();

    /**
     * Zookeeper must be running.
     */
    public static transient final IServiceConstraint ZOOKEEPER = new JiniRunningConstraint();

    /**
     * The {@link TransactionServer} must be running - note this service also
     * provides the {@link ITimestampService} implementation.
     */
    public static transient final IServiceConstraint TX = new TXRunningConstraint();

    /**
     * The {@link MetadataServer} must be running.
     */
    public static transient final IServiceConstraint MDS = new JiniRunningConstraint();

    /**
     * Constraint that jini must be running (one or more service registrars must
     * have been discovered).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JiniRunningConstraint extends ServiceDependencyConstraint {

        /**
         * 
         */
        private static final long serialVersionUID = 9207209964254849382L;

        public boolean allow(JiniFederation fed) throws Exception {

            if (fed.getDiscoveryManagement().getRegistrars().length == 0) {

                if (INFO)
                    log.info("No registrars have been discovered");

                return false;

            }

            // return true if any registrars have been discovered.
            return true;

        }

    };

    /**
     * Constraint that zookeeper must be running (the {@link ZooKeeper} client
     * is {@link ZooKeeper.States#CONNECTED}).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ZookeeperRunningConstraint extends
            ServiceDependencyConstraint {

        /**
         * 
         */
        private static final long serialVersionUID = -9179574081166981787L;

        public boolean allow(JiniFederation fed) throws Exception {

            final ZooKeeper.States state = fed.getZookeeper().getState();

            switch (state) {

            case CONNECTED:
                return true;

            default:
                if (INFO)
                    log.info("Zookeeper not connected: state=" + state);

                return false;

            }

        }

    }

    /**
     * The {@link ITransactionService} must be discovered.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TXRunningConstraint extends ServiceDependencyConstraint {

        /**
         * 
         */
        private static final long serialVersionUID = 6590113180404519952L;

        public boolean allow(JiniFederation fed) throws Exception {

            if (fed.getTransactionService() == null) {
            
                if(INFO)
                    log.info("Not discovered: "
                            + ITransactionService.class.getName());
                
                return false;
                
            }
            
            return true;
            
        }
        
    }


    /**
     * The {@link IMetadataService} must be discovered.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MDSRunningConstraint extends ServiceDependencyConstraint {

        /**
         * 
         */
        private static final long serialVersionUID = -1983273198622764005L;

        public boolean allow(JiniFederation fed) throws Exception {

            if (fed.getMetadataService() == null) {
            
                if (INFO)
                    log.info("Not discovered: "
                            + IMetadataService.class.getName());
                
                return false;
                
            }
            
            return true;
            
        }
        
    }

}
