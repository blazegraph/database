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
import java.util.concurrent.atomic.AtomicBoolean;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;

/**
 * Test suite for {@link SequenceTask}
 * 
 * @todo test that {@link SequenceTask#newSequence(AbstractTask[])}
 *       produces the expected data and correctly rejects tasks that are
 *       not compatible.
 * 
 * @todo test that a sequence of unisolated write tasks is atomic.
 * 
 * @todo test sequence of transaction tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSequenceTask extends ProxyTestCase {

    /**
     * 
     */
    public TestSequenceTask() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSequenceTask(String arg0) {
        super(arg0);
    }

    /**
     * Submits an unisolated task to the write service and verifies that it
     * executes. The task writes some data and we verify that the commit counter
     * is updated.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_writeService01() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        final UUID indexUUID = UUID.randomUUID();
        
        Future<Object> future = journal.submit(SequenceTask.newSequence(
                
                new AbstractTask[] {

                new RegisterIndexTask(journal,resource[0], new IndexMetadata(
                        resource[0], indexUUID)),
                                
                new AbstractTask(journal, ITx.UNISOLATED, resource) {

                    /**
                     * The task just sets a boolean value and returns the name
                     * of the sole resource. It does not actually read or write
                     * on anything.
                     */
                    protected Object doTask() throws Exception {

                        ran.compareAndSet(false, true);
                        
                        return getOnlyResource();

                    }
                }
                
        }));

        // the test task returns the resource as its value.
        assertEquals("result", //
                new Object[] {//
                indexUUID,//
                resource[0] //
                },//
                (Object[]) future.get() //
                );
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was changed.
         */
        assertEquals("commit counter unchanged?",
                commitCounterBefore + 1, journal.getRootBlockView()
                        .getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

    /**
     * Submits an unisolated {@link SequenceTask} to the write service and
     * verifies that it executes. The 1st task in the sequence writes creates an
     * index, the 2nd writes some data on that index, and the last one sets a
     * flag to indicate that the tasks ran. We verify that the commit counter is
     * updated.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_submit_writeService02() throws InterruptedException, ExecutionException {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);
        
        final String[] resource = new String[]{"foo"};
        
        final long commitCounterBefore = journal.getRootBlockView().getCommitCounter();

        final AtomicBoolean ran = new AtomicBoolean(false);
        
        final UUID indexUUID = UUID.randomUUID();
        
        Future<Object> future = journal.submit(SequenceTask.newSequence(
                
                new AbstractTask[] {

                new RegisterIndexTask(journal, resource[0], new IndexMetadata(
                        resource[0], indexUUID)),


                new AbstractTask(journal, ITx.UNISOLATED, resource) {

                    /**
                     * Write on the index that we just created.
                     */
                    protected Object doTask() throws Exception {

                        IIndex ndx = getIndex(getOnlyResource());
                        
                        ndx.insert(new byte[]{1,2,3}, new byte[]{1,2,3});
                        
                        // return an arbitrary value.
                        return Integer.valueOf(5);

                    }
                
                },
                                                
                new AbstractTask(journal, ITx.UNISOLATED, resource) {

                    /**
                     * The task just sets a boolean value and returns the name of the
                     * sole resource. It does not actually read or write on anything.
                     */
                    protected Object doTask() throws Exception {

                        ran.compareAndSet(false, true);
                        
                        return getOnlyResource();

                    }
                    
                }
                
        }));

        // the test task returns the resource as its value.
        assertEquals("result", new Object[] {//
                indexUUID,//
                Integer.valueOf(5),//
                resource[0] //
                },//
                (Object[]) future.get() //
        );
        
        /*
         * make sure that the flag was set (not reliably set until we get() the
         * future).
         */
        assertTrue("ran",ran.get());

        /*
         * Verify that the commit counter was changed.
         */
        assertEquals("commit counter unchanged?",
                commitCounterBefore + 1, journal.getRootBlockView()
                        .getCommitCounter());

        journal.shutdown();

        journal.delete();

    }

}
