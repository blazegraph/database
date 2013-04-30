/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.zk.ZKQuorumImpl;

/**
 * Class extends {@link HAJournal} and allows the unit tests to play various
 * games with the services, simulating a variety of different kinds of problems.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAJournalTest extends HAJournal {

    private static final Logger log = Logger.getLogger(HAJournal.class);

    public HAJournalTest(final HAJournalServer server,
            final Configuration config,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException, IOException {

        super(server, config, quorum);

    }

    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

        return new HAGlueTestService(serviceId);
      
    }
    
    /**
     * A {@link Remote} interface for new methods published by the service.
     */
    public static interface HAGlueTest extends HAGlue {

        /**
         * Logs a "hello world" message.
         */
        public void helloWorld() throws IOException;
        
        /**
         * Force the end point to enter into an error state from which it will
         * naturally move back into a consistent state.
         * <p>
         * Note: This method is intended primarily as an aid in writing various HA
         * unit tests.
         */
        public Future<Void> enterErrorState() throws IOException;
        
        /**
         * This method may be issued to force the service to close and then reopen
         * its zookeeper connection. This is a drastic action which will cause all
         * <i>ephemeral</i> tokens for that service to be retracted from zookeeper.
         * When the service reconnects, it will reestablish those connections.
         * <p>
         * Note: This method is intended primarily as an aid in writing various HA
         * unit tests.
         * 
         * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
         */
        public Future<Void> bounceZookeeperConnection() throws IOException;
        
    }
    
    /**
     * Class extends the public RMI interface of the {@link HAJournal}.
     * <p>
     * Note: Any new RMI methods must be (a) declared on an interface; and (b)
     * must throw {@link IOException}.
     */
    protected class HAGlueTestService extends HAJournal.HAGlueService implements
            HAGlueTest {

        protected HAGlueTestService(final UUID serviceId) {

            super(serviceId);

        }

        @Override
        public void helloWorld() throws IOException {

            log.warn("Hello world!");

        }
        
        @Override
        public Future<Void> enterErrorState() {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new EnterErrorStateTask(), null/* result */);

            ft.run();

            return getProxy(ft);

        }

        private class EnterErrorStateTask implements Runnable {

            public void run() {

                @SuppressWarnings("unchecked")
                final HAQuorumService<HAGlue, HAJournal> service = (HAQuorumService<HAGlue, HAJournal>) getQuorum()
                        .getClient();

                // Note: Local method call on AbstractJournal.
                final UUID serviceId = getServiceId();

                haLog.warn("ENTERING ERROR STATE: " + serviceId);

                service.enterErrorState();

            }

        }

        @Override
        public Future<Void> bounceZookeeperConnection() {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new BounceZookeeperConnectionTask(), null/* result */);

            ft.run();
            
            return getProxy(ft);

        }

        private class BounceZookeeperConnectionTask implements Runnable {

            @SuppressWarnings("rawtypes")
            public void run() {

                if (getQuorum() instanceof ZKQuorumImpl) {

                    // Note: Local method call on AbstractJournal.
                    final UUID serviceId = getServiceId();
                    
                    try {

                        haLog.warn("BOUNCING ZOOKEEPER CONNECTION: "
                                + serviceId);

                        // Close the current connection (if any).
                        ((ZKQuorumImpl) getQuorum()).getZookeeper().close();

                        // Obtain a new connection.
                        ((ZKQuorumImpl) getQuorum()).getZookeeper();

                        haLog.warn("RECONNECTED TO ZOOKEEPER: " + serviceId);

                    } catch (InterruptedException e) {

                        // Propagate the interrupt.
                        Thread.currentThread().interrupt();

                    }

                }
            }

        }

    } // HAGlueTestService

}
