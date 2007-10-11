/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test suite for {@link SequenceTask}
 * 
 * @todo test that {@link SequenceTask#newSequence(AbstractIndexTask[])}
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
        
        Future<Object> future = journal.submit(SequenceTask.newSequence(
                
                new AbstractIndexTask[] {

                new RegisterIndexTask(journal, resource[0],
                                        new UnisolatedBTree(journal, UUID
                                                .randomUUID())),
                new AbstractIndexTask(journal,
                        ITx.UNISOLATED, false/*readOnly*/, resource) {

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
        assertEquals("result", //
                new Object[] {//
                Boolean.TRUE,//
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
        
        Future<Object> future = journal.submit(SequenceTask.newSequence(
                
                new AbstractIndexTask[] {

                new RegisterIndexTask(journal, resource[0],
                                        new UnisolatedBTree(journal, UUID
                                                .randomUUID())),


                new AbstractIndexTask(journal,
                        ITx.UNISOLATED, false/*readOnly*/, resource) {

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
                                                
                new AbstractIndexTask(journal,
                        ITx.UNISOLATED, false/*readOnly*/, resource) {

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
                Boolean.TRUE,//
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
