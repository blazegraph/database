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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal.ha;

import java.io.File;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import com.bigdata.LRUNexus;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.quorum.MockQuorumFixture;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;

/**
 * <p>
 * Abstract harness for testing under a variety of configurations. In order to
 * test a specific configuration, create a concrete instance of this class. The
 * configuration can be described using a mixture of a <code>.properties</code>
 * file of the same name as the test class and custom code.
 * </p>
 * <p>
 * When debugging from an IDE, it is very helpful to be able to run a single
 * test case. You can do this, but you MUST define the property
 * <code>testClass</code> as the name test class that has the logic required
 * to instantiate and configure an appropriate object manager instance for the
 * test.
 * </p>
 */
abstract public class AbstractHAJournalTestCase
    extends AbstractJournalTestCase
{

    //
    // Constructors.
    //

    public AbstractHAJournalTestCase() {}
    
    public AbstractHAJournalTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * The replication factor for the quorum. This is initialized in
     * {@link #setUp(ProxyTestCase)} so you can override it. The default is
     * <code>3</code>, which is a highly available quorum.
     */
    protected int k;

    /**
     * The replication count (#of journals) for the quorum. This is initialized
     * in {@link #setUp(ProxyTestCase)} so you can override it. The default is
     * <code>3</code>, which is the same as the default replication factor
     * {@link #k}.
     */
    protected int replicationCount;
    /**
     * The fixture provides a mock of the distributed quorum state machine.
     */
    protected MockQuorumFixture fixture = null;
    /**
     * The logical service identifier.
     */
    protected String logicalServiceId = null;
    /**
     * The {@link Journal}s which are the members of the logical service.
     */
    private Journal[] stores = null;

    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        super.setUp(testCase);
        
//        if(log.isInfoEnabled())
//        log.info("\n\n================:BEGIN:" + testCase.getName()
//                + ":BEGIN:====================");

        fixture = new MockQuorumFixture();
        fixture.start();
        logicalServiceId = "logicalService_" + getName();

        k = 3;
        
        replicationCount = 3;
        
    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        if (stores != null) {
            for (Journal store : stores) {
                if (store != null) {
                    store.destroy();
                }
            }
        }

        if(fixture!=null) {
            fixture.terminate();
            fixture = null;
        }
        
        logicalServiceId = null;
        
        super.tearDown(testCase);

    }

    /**
     * Note: Due to the manner in which the {@link MockQuorumManager} is
     * initialized, the elements in {@link #stores} will be <code>null</code>
     * initially. This should be Ok as long as the test gets fully setup before
     * you start making requests of the {@link QuorumManager} or the
     * {@link Quorum}.
     */
    @Override
    protected Journal getStore(final Properties properties) {

        stores = new Journal[replicationCount];

        for (int i = 0; i < replicationCount; i++) {

            stores[i] = newJournal(properties);

        }

        /*
         * Initialize the master first. The followers will get their root blocks
         * from the master.
         */
//        fixture.join(stores[0]);
//        fixture.join(stores[1]);
//        fixture.join(stores[2]);
//
//        stores[1].takeRootBlocksFromLeader();
//        stores[2].takeRootBlocksFromLeader();

        /*
         * @todo we probably have to return the service which joined as the
         * leader from getStore().
         */
        final Quorum<HAGlue, QuorumService<HAGlue>> q = stores[0].getQuorum();

        try {
        
            final long token = q.awaitQuorum(1L,TimeUnit.SECONDS);
            assertEquals(token, stores[1].getQuorum().awaitQuorum(1L,TimeUnit.SECONDS));
            assertEquals(token, stores[2].getQuorum().awaitQuorum(1L,TimeUnit.SECONDS));

            q.getClient().assertLeader(token);

            assertEquals(k, q.getMembers().length);
        
        } catch (TimeoutException ex) {
        
            throw new RuntimeException(ex);

        } catch (InterruptedException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        // return the master.
        return stores[0];

    }

    protected Journal newJournal(final Properties properties) {

        /*
         * Initialize the HA components.
         */

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = newQuorum();

        final HAJournal jnl = new HAJournal(properties, quorum);

        /*
         * FIXME This probably should be a constant across the life cycle of the
         * service, in which case it needs to be elevated outside of this method
         * which is used both to open and re-open the journal.
         */
        final UUID serviceId = UUID.randomUUID();

        /*
         * Set the client on the quorum.
         * 
         * FIXME The client needs to manage the quorumToken and various other
         * things.
         */
        quorum.start(newQuorumService(logicalServiceId, serviceId, jnl
                .newHAGlue(serviceId), jnl));

//      // discard the current write set.
//      abort();
//      
//      // set the quorum object.
//      this.quorum.set(quorum);
//
//      // save off the current token (typically NO_QUORUM unless standalone).
//      quorumToken = quorum.token();

        /*
         * Tell the actor to try and join the quorum. It will join iff our
         * current root block can form a simple majority with the other services
         * in the quorum.
         */
        final QuorumActor<?, ?> actor = quorum.getActor();
        try {
            
            actor.memberAdd();
            fixture.awaitDeque();
            
            actor.pipelineAdd();
            fixture.awaitDeque();
            
            actor.castVote(jnl.getLastCommitTime());
            fixture.awaitDeque();

        } catch (InterruptedException ex) {
        
            throw new RuntimeException(ex);
            
        }
        
        return jnl;
        
//        return new Journal(properties) {
//
//            protected Quorum<HAGlue, QuorumService<HAGlue>> newQuorum() {
//
//                return AbstractHAJournalTestCase.this.newQuorum();
//
//            };
//
//        };

    }

    private static class HAJournal extends Journal {

        /**
         * @param properties
         */
        public HAJournal(Properties properties,
                Quorum<HAGlue, QuorumService<HAGlue>> quorum) {
            super(properties, quorum);
        }
        
        public HAGlue newHAGlue(final UUID serviceId) {
         
            return super.newHAGlue(serviceId);
            
        }
        
    }
    
    protected Quorum<HAGlue, QuorumService<HAGlue>> newQuorum() {

        return new MockQuorumFixture.MockQuorum<HAGlue, QuorumService<HAGlue>>(
                k, fixture);

    };

    /**
     * Factory for the {@link QuorumService} implementation.
     * 
     * @param remoteServiceImpl
     *            The object that implements the {@link Remote} interfaces
     *            supporting HA operations.
     */
    protected QuorumServiceBase<HAGlue, AbstractJournal> newQuorumService(
            final String logicalServiceId,
            final UUID serviceId, final HAGlue remoteServiceImpl,
            final AbstractJournal store) {

        return new QuorumServiceBase<HAGlue, AbstractJournal>(logicalServiceId,
                serviceId, remoteServiceImpl, store) {

            /**
             * Only the local service implementation object can be resolved.
             */
            @Override
            public HAGlue getService(final UUID serviceId) {
                
                return (HAGlue) fixture.getService(serviceId);
                
            }

            /**
             * FIXME handle replicated writes. Probably just dump it on the
             * jnl's WriteCacheService. Or maybe wrap it back up using a
             * WriteCache and let that lay it down onto the disk.
             */
            @Override
            protected void handleReplicatedWrite(HAWriteMessage msg,
                    ByteBuffer data) throws Exception {

                
//                new WriteCache() {
//                    
//                    @Override
//                    protected boolean writeOnChannel(ByteBuffer buf, long firstOffset,
//                            Map<Long, RecordMetadata> recordMap, long nanos)
//                            throws InterruptedException, TimeoutException, IOException {
//                        // TODO Auto-generated method stub
//                        return false;
//                    }
//                };
                
                throw new UnsupportedOperationException();
            }
        };

    }

    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed or if the store can not be
     *                re-opened, e.g., from failure to obtain a file lock, etc.
     */
    @Override
    protected Journal reopenStore(final Journal store) {

        if (stores[0] != store)
            throw new AssertionError();
        
        for (int i = 0; i < stores.length; i++) {

            Journal aStore = stores[i];

            if (LRUNexus.INSTANCE != null) {
                /*
                 * Drop the record cache for this store on reopen. This makes it
                 * easier to find errors related to a difference in the bytes on
                 * the disk versus the bytes in the record cache.
                 */
                LRUNexus.INSTANCE.deleteCache(aStore.getUUID());
            }

            // close the store.
            aStore.close();

            if (!aStore.isStable()) {

                throw new UnsupportedOperationException(
                        "The backing store is not stable");

            }

            // Note: clone to avoid modifying!!!
            final Properties properties = (Properties) getProperties().clone();

            // Turn this off now since we want to re-open the same store.
            properties.setProperty(Options.CREATE_TEMP_FILE, "false");

            // The backing file that we need to re-open.
            final File file = aStore.getFile();

            if (file == null)
                throw new AssertionError();

            // Set the file property explicitly.
            properties.setProperty(Options.FILE, file.toString());

            stores[i] = newJournal(properties);

        }

        return stores[0];
        
    }

//    /**
//     * Begin to run as part of a highly available {@link Quorum}.
//     * 
//     * @param newQuorum
//     *            The {@link Quorum}.
//     */
//    public void joinQuorum(final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {
//
//        if (quorum == null)
//            throw new IllegalArgumentException();
//
//        final WriteLock lock = _fieldReadWriteLock.writeLock();
//
//        lock.lock();
//
//        try {
//
//            if (this.quorum.get() != null) {
//                // Already running with some quorum.
//                throw new IllegalStateException();
//            }
//
//            // discard the current write set.
//            abort();
//            
//            // set the quorum object.
//            this.quorum.set(quorum);
//
//            // save off the current token (typically NO_QUORUM unless standalone).
//            quorumToken = quorum.token();
//
//            /*
//             * Tell the actor to try and join the quorum. It will join iff our
//             * current root block can form a simple majority with the other
//             * services in the quorum.
//             */
//            final QuorumActor<?,?> actor = quorum.getActor();
//            actor.memberAdd();
//            actor.pipelineAdd();
//            actor.castVote(getRootBlockView().getLastCommitTime());
//            
//        } catch (Throwable e) {
//
//            throw new RuntimeException(e);
//
//        } finally {
//
//            lock.unlock();
//
//        }
//
//    }
    
}
