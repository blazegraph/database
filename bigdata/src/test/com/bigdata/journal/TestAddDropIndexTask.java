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
 * Created on Oct 10, 2007
 */

package com.bigdata.journal;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ConcurrentJournal.Options;

/**
 * Test suite for {@link RegisterIndexTask} and {@link DropIndexTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAddDropIndexTask extends ProxyTestCase {

    /**
     * 
     */
    public TestAddDropIndexTask() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAddDropIndexTask(String arg0) {
        super(arg0);
    }

    /**
     * Test ability to create a {@link ConcurrentJournal}, submit an unisolated
     * write task that creates a named index shutdown the journal. The journal
     * is then re-opened and we verify that the registered index is restart
     * safe.  Finally we drop the index, close the journal and then re-open it
     * again to verify that the drop index operation was restart safe.
     */
    public void test_addDropIndex() {

        // Note: wrapped since we modify the properties during the test!!!
        Properties properties = new Properties(getProperties());
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";

        final UUID indexUUID = UUID.randomUUID();
        
        /*
         * Run task to register a named index.
         */
        {
         
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();
            
            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, indexUUID)));

            try {

                log.info("Resolving future for task.");
                
                assertEquals( "indexUUID", indexUUID, (UUID)future.get() );

                log.info("Resolved future");
                
                /*
                 * This verifies that the write task did not return control to
                 * the caller until the write set of that task was committed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * reopen (iff backed by stable store).
         */

        if(journal.isStable()) {

            journal.shutdown();

            properties.setProperty(Options.CREATE_TEMP_FILE,"false");
            properties.setProperty(Options.FILE,journal.getFile().toString());

            journal = new Journal(properties);
            
        }
        
        /*
         * Verify that the index was registered.
         */
        {

            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new AbstractTask(
                    journal, ITx.UNISOLATED, true/* readOnly */, name) {

                protected Object doTask() throws Exception {

                    /*
                     * Note: Throws an exception if the index is not registered.
                     */

                    IIndex ndx = getIndex(name);
                    
                    assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());
                    
                    return null;

                }
            });

            try {

                // return value should be [null] for this task.
                
                assertNull( future.get() );

                /*
                 * The commit counter MUST NOT have been changed since we ran a
                 * read-only task.
                 */
                
                assertEquals("commit counter changed?",
                        commitCounterBefore, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Drop the index.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));
            
            try {

                // return is true iff the index was pre-existing (and therefore
                // was dropped).
                
                assertTrue( "Index did not exist?", (Boolean)future.get() );

                /*
                 * This verifies that the write task did not return control to
                 * the caller until the write set of that task was committed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore + 1, journal.getRootBlockView()
                                .getCommitCounter());
                
            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        /*
         * Re-open the journal (iff backed by stable store). 
         */
        if(journal.isStable()) {
        
            journal.shutdown();
            
            journal = new Journal(properties);
            
        }
        
        /*
         * Verify that the index is gone.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

            Future<Object> future = journal.submit(new AbstractTask(
                    journal, ITx.UNISOLATED, true/* readOnly */, name) {

                protected Object doTask() throws Exception {

                    /*
                     * Note: Throws an exception if the index is not registered.
                     */

                    try {

                        getIndex(name);
                        
                        fail("Expecting: "+NoSuchIndexException.class);
                        
                    } catch(NoSuchIndexException ex) {
                        
                        System.err.println("Ignoring expected exception: "+ex);
                        
                    }

                    return null;

                }
            });

            try {

                // return value should be [null] for this task.
                
                assertNull( future.get() );

                /*
                 * We ran a read-only task so the commit counter MUST NOT have
                 * been changed.
                 */
         
                assertEquals("commit counter unchanged?",
                        commitCounterBefore, journal.getRootBlockView()
                                .getCommitCounter());

            } catch (InterruptedException ex) {

                fail("Not expecting: " + ex, ex);

            } catch (ExecutionException ex) {

                fail("Not expecting: " + ex, ex);

            }
            
        }

        // shutdown again.
        journal.shutdown();
        
        /*
         * Delete the backing store to clean up after the test.
         */
        
        journal.delete();
        
    }

    /**
     * Test registers an index and then verifies that a second
     * {@link RegisterIndexTask} will return <code>false</code> since the
     * index already exists.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_addDropIndex_twice() throws InterruptedException, ExecutionException {

        Properties properties = new Properties();
        
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";
        
        final UUID indexUUID = UUID.randomUUID();
        
        /*
         * Run task to register a named index.
         */
        {

            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, indexUUID)));

            assertEquals("indexUUID", indexUUID, (UUID) future.get());

            /*
             * This verifies that the write task did not return control to the
             * caller until the write set of that task was committed.
             */

            assertEquals("commit counter unchanged?", commitCounterBefore + 1,
                    journal.getRootBlockView().getCommitCounter());

            // verify access to the index.
            assertNotNull(journal.getIndex(name));
            assertEquals(indexUUID,journal.getIndex(name).getIndexUUID());
            
        }

        /*
         * Run another task to re-register the same named index.
         */
        {
         
            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new RegisterIndexTask(
                    journal, name, new UnisolatedBTree(journal, indexUUID)));

            // Note: the UUID for the pre-existing index is returned.
            assertEquals("indexUUID", indexUUID, (UUID) future.get());

            /*
             * This verifies that no commit was performed since no data was
             * actually written on the store because the index was pre-existing.
             */

            assertEquals("commit counter changed?", commitCounterBefore,
                    journal.getRootBlockView().getCommitCounter());

        }

        /*
         * Now drop the index.
         */
        {
            
            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));

            // should return true if the index was dropped.
            assertTrue("Index did not exist?", (Boolean) future.get());

            /*
             * Verify that a commit was performed.
             */

            assertEquals("commit counter unchanged?", commitCounterBefore + 1,
                    journal.getRootBlockView().getCommitCounter());

        }

        /*
         * Now drop the index again.
         */
        {

            final long commitCounterBefore = journal.getRootBlockView()
                    .getCommitCounter();

            Future<Object> future = journal.submit(new DropIndexTask(journal,
                    name));

            // should return false since the index does not exist.
            assertFalse("Index exists?", (Boolean) future.get());

            /*
             * Verify that a commit was NOT performed since no data was written
             * by the task.
             */

            assertEquals("commit counter changed?", commitCounterBefore,
                    journal.getRootBlockView().getCommitCounter());

        }

        journal.shutdown();

        journal.delete();

    }

    /**
     * Test attempt operations against a new journal (nothing committed) and
     * verify that we see {@link NoSuchIndexException}s rather than something
     * odder. This is an edge case since {@link Journal#getCommitRecord()} will
     * have 0L for all root addresses until the first commit - this means that
     * the {@link AbstractJournal#name2Addr} can not be loaded from the commit
     * record!
     * 
     * @throws InterruptedException
     */
    public void test_NoSuchIndexException() throws InterruptedException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String name = "abc";

        Future<Object> future = journal.submit(new AbstractTask(journal,ITx.UNISOLATED,true/*readOnly*/,name){

            protected Object doTask() throws Exception {
                
                getIndex(name);
                
                return null;
                
            }});

        try {

            future.get();

            fail("Expecting wrapped "+NoSuchIndexException.class);
            
        } catch(ExecutionException ex) {
            
            if(ex.getCause() instanceof NoSuchIndexException ) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            } else {

                fail("Expecting wrapped "+NoSuchIndexException.class);
                
            }

        }
        
        journal.shutdown();
        
        journal.delete();

    }
    
}
