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
 * Created on Oct 16, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;

/**
 * Test suite for correct handling of {@link ClosedByInterruptException}s. When
 * a commit group is aborted, the {@link Thread}s for the tasks in that commit
 * group are interrupted. If a task was in the midst of an IO operation on a
 * {@link Channel} then the channel will be asynchronously closed by the JDK.
 * Since we use a {@link FileChannel} to access the backing store, this means
 * that we need to re-open the backing store transparently so that we can
 * continue operations after the commit group was aborted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestClosedByInterruptException extends ProxyTestCase {

    /**
     * 
     */
    public TestClosedByInterruptException() {
        super();
    }

    /**
     * @param name
     */
    public TestClosedByInterruptException(String name) {
        super(name);
    }

    public void test_handleClosedByInterruptException()
            throws InterruptedException, ExecutionException {

        Journal journal = new Journal(getProperties());

        doTest(journal);

        journal.closeAndDelete();

    }

    static protected void doTest(Journal journal) throws InterruptedException,
            ExecutionException {
        
        final String[] resource = new String[]{"foo"};//,"bar","baz"};
        
        // register the indices.
        for(int i=0; i<resource.length; i++){
            journal.registerIndex(resource[i]);
        }
        // and commit.
        journal.commit();

        /*
         * Run a task that waits for 10 seconds or until interrupted. It will be
         * in the same commit group as the next task since it is just waiting
         * around.
         * 
         * Note: This MUST run on the write service so that it is the commit
         * group. However it MUST NOT declare a lock on the named index or
         * the 'interrupt' task will not get to execute.  Also, do NOT "get()"
         * this task since that will block you and the 'interrupt' task will
         * not run.
         */
        journal.submit(new AbstractTask(journal,ITx.UNISOLATED,false/*readOnly*/,new String[]{}){

            protected Object doTask() throws Exception {
                
                // sleep for 10 seconds.
                
                Thread.sleep(10*1000);
                
                throw new AssertionError("Not expecting to wake up.");
                
            }});
        
        /*
         * Run a task that interrupts itself causing the commit group to be
         * discarded.
         */
        try {
            journal.submit(new InterruptMyselfTask(journal,ITx.UNISOLATED,false/*readOnly*/,resource[0])).get();
            fail("Not expecting success");
        } catch(ExecutionException ex) {
            log.warn(ex);
            assertTrue(isInnerCause(ex, ClosedByInterruptException.class));
        }

        /*
         * write on the store and flush the store to disk in order to provoke an
         * exception if the channel is still closed.
         */

        journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
        
        journal.force(true);
        
    }
    
    private static class InterruptMyselfTask extends AbstractTask {

        /**
         * @param journal
         * @param startTime
         * @param readOnly
         * @param resource
         */
        protected InterruptMyselfTask(ConcurrentJournal journal, long startTime, boolean readOnly, String resource) {
            super(journal, startTime, readOnly, resource);
        }

        protected Object doTask() throws Exception {
            
            // interrupt ourselves.
            Thread.currentThread().interrupt();
            
            /*
             * attempt to get and write on the index - this will notice the
             * interrupt and cause an exception to be thrown.
             */
            BTree ndx = (BTree) getIndex(getOnlyResource());
            
            ndx.insert(new byte[]{},new byte[]{});
            
            // force flush of index to store
            ndx.write();
            
            // force flush of store to disk.
            journal.force(true);
            
            throw new AssertionError("Not expecting to access index.");
            
        }
        
    }
    
    /**
     * Runs the test a bunch of times to see if it fails.
     * 
     * @param args
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        final int limit = 100;
        
        for (int i = 0; i < limit; i++) {

            Properties properties = new Properties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.CREATE_TEMP_FILE, "true");

            properties.setProperty(Options.DELETE_ON_EXIT, "true");

            doTest(new Journal(properties));

        }
        
        System.err.println("Ok");
        
    }
    
}
