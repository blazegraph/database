/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Oct 15, 2007
 */

package com.bigdata.journal;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;

/**
 * Test suite for hierarchical locking of indices based on namespace prefixes.
 * This test suite was introduced as part of the support for group commit for
 * the RDF database layer in the non-scale-out modes. In order to be able to run
 * RDF operations using job-based concurrency, we need to predeclare the
 * namespace (rather than the individual indices) and then isolate all indices
 * spanned by that namespace.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
 *      Concurrent unisolated operations against multiple KBs </a>
 * 
 *      FIXME GROUP COMMIT (hierarchical locking): We need to test each
 *      different kind of index access here (unisolated, isolated by a
 *      read/write tx, and isolated by a read-only tx, and read-historical (no
 *      isolation)).
 */
public class TestHierarchicalLockingTasks extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestHierarchicalLockingTasks() {
        super();
    }

    /**
     * @param name
     */
    public TestHierarchicalLockingTasks(final String name) {
        super(name);
    }

    /**
     * Test creates several named indices some of which have a shared namespace
     * prefix and verifies that a lock declared for the namespace prefix permits
     * that task to access any index in that namespace. The test also verifies
     * that an index that does not share the namespace prefix may not be
     * accessed by the task.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_hierarchicalLocking_001() throws InterruptedException,
            ExecutionException {

        // namespace for declared locks.
        final String[] namespace = new String[] { "foo" };
        
        // indices that we create, write on, and verify.
        final String[] allowed_indices = new String[] { "foo.bar", "foo.baz" };
        
        // indices that are outside of the declared namespace. we verify that we
        // can not access these indices.
        final String[] disallowed_indices = new String[] { "goo", "goo.bar" };

        final Journal journal = getStore();
        
        try {

            /*
             * Create indices that we should not be able to see when holding
             * just the lock for the namespace.
             */
            for (int i = 0; i < disallowed_indices.length; i++) {

                final String name = disallowed_indices[i];

                journal.submit(new RegisterIndexTask(journal
                        .getConcurrencyManager(), name, new IndexMetadata(name,
                        UUID.randomUUID()))).get();

            }

            /*
             * Create indices that we should be able to see when holding the
             * lock for the namespace.
             */
            for (int i = 0; i < allowed_indices.length; i++) {

                final String name = allowed_indices[i];

                journal.submit(new RegisterIndexTask(journal
                        .getConcurrencyManager(), name, new IndexMetadata(name,
                        UUID.randomUUID()))).get();

            }

            /*
             * Submit task holding the lock for the namespace and verify that we
             * can see the indices that are spanned by the namespace, but not
             * those that are not spanned by the namespace.
             */

            journal.submit(
                    new AbstractTask<Void>(journal.getConcurrencyManager(),
                            ITx.UNISOLATED, namespace) {

                        @Override
                        protected Void doTask() throws Exception {

                            // Verify access to the indices in that namespace.
                            for (String name : allowed_indices) {

                                assertNotNull(name, getIndex(name));

                            }

                            /*
                             * Verify no access to the indices outside of that
                             * namespace.
                             */
                            for (String name : disallowed_indices) {

                                try {
                                    // Attempt to access index.
                                    getIndex(name);
                                    fail("Expecting: "
                                            + IllegalStateException.class
                                            + " for " + name);
                                } catch (IllegalStateException ex) {
                                    // log and ignore expected exception.
                                    if (log.isInfoEnabled())
                                        log.info("Ignoring expected exception");
                                }

                            }

                            // Done.
                            return null;
                        }
                    }).get();

        } finally {

            journal.destroy();

        }

    }

    /**
     * Unit test for hierarchical locking verifies that we can declared a
     * namespace for a task which then creates multiple indices spanned by that
     * namespace.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_hierarchicalLocking_create_destroy()
            throws InterruptedException, ExecutionException {

        // namespace for declared locks.
        final String[] namespace = new String[] { "foo" };

        // indices that we create, write on, and verify.
        final String[] allowed_indices = new String[] { "foo.bar", "foo.baz" };

        final Journal journal = getStore();

        try {

            final UUID[] uuids = journal.submit(
                    new AbstractTask<UUID[]>(journal.getConcurrencyManager(),
                            ITx.UNISOLATED, namespace) {

                        @Override
                        protected UUID[] doTask() throws Exception {

                            final UUID[] indexUUIDs = new UUID[allowed_indices.length];
                            
                            /*
                             * Create indices that we should be able to see when
                             * holding the lock for the namespace.
                             */
                            for (int i = 0; i < allowed_indices.length; i++) {

                                final String name = allowed_indices[i];

                                IIndex ndx = getJournal().getIndex(name);
                                
                                if (ndx != null) {

                                    final UUID indexUUID = ndx.getIndexMetadata().getIndexUUID();

                                    if (log.isInfoEnabled())
                                        log.info("Index exists: name=" + name + ", indexUUID=" + indexUUID);

                                    indexUUIDs[i] = indexUUID;

                                }

                                // register the index.
                                ndx = getJournal().registerIndex(
                                        name,
                                        new IndexMetadata(name, UUID
                                                .randomUUID()));

                                final UUID indexUUID = ndx.getIndexMetadata().getIndexUUID();

                                if (log.isInfoEnabled())
                                    log.info("Registered index: name=" + name + ", class="
                                            + ndx.getClass() + ", indexUUID=" + indexUUID);

                                indexUUIDs[i] = indexUUID;
                                
                            }

                            // Done
                            return indexUUIDs;
                        }

                    }).get();

            // should be non-null.
            assertNotNull(uuids);
            
            // Should be non-null.
            for (UUID uuid : uuids) {
             
                assertNotNull(uuid);
                
            }

            /*
             * Verify access to the newly created indices.
             */
            journal.submit(
                    new AbstractTask<Void>(journal.getConcurrencyManager(),
                            ITx.UNISOLATED, namespace) {

                        @Override
                        protected Void doTask() throws Exception {
                            
                            /*
                             * Verify access to the newly created indices.
                             */
                            for (int i = 0; i < allowed_indices.length; i++) {

                                final String name = allowed_indices[i];

                                final ILocalBTreeView ndx = getIndex(name);

                                assertNotNull(ndx);

                                assertEquals(0L, ndx.rangeCount());

                                // add a key.
                                ndx.insert(new byte[] {},
                                        new byte[] { (byte) i });
                                
                            }

                            // Done
                            return null;
                        }

                    }).get();

            /*
             * Destroy the newly created indices.
             */
            journal.submit(
                    new AbstractTask<Void>(journal.getConcurrencyManager(),
                            ITx.UNISOLATED, namespace) {

                        @Override
                        protected Void doTask() throws Exception {

                            /*
                             * Create indices that we should be able to see when
                             * holding the lock for the namespace.
                             */
                            for (int i = 0; i < allowed_indices.length; i++) {

                                final String name = allowed_indices[i];

                                getJournal().dropIndex(name);

                            }

                            // Done
                            return null;
                        }

                    }).get();

            /*
             * Verify that the indices can no longer be accessed.
             */
            journal.submit(
                    new AbstractTask<Void>(journal.getConcurrencyManager(),
                            ITx.UNISOLATED, namespace) {

                        @Override
                        protected Void doTask() throws Exception {
                            
                            /*
                             * Verify no access to the dropped indices.
                             */
                            for (int i = 0; i < allowed_indices.length; i++) {

                                final String name = allowed_indices[i];

                                try {
                                    // Attempt to access index.
                                    getIndex(name);
                                    fail("Expecting: "
                                            + IllegalStateException.class
                                            + " for " + name);
                                } catch (IllegalStateException ex) {
                                    // log and ignore expected exception.
                                    if (log.isInfoEnabled())
                                        log.info("Ignoring expected exception");
                                }

                            }

                            // Done
                            return null;
                        }

                    }).get();

        } finally {

            journal.destroy();

        }

    }

}
