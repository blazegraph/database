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
 * Created on Oct 16, 2007
 */

package com.bigdata.journal;

import java.awt.image.BufferStrategy;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for correct handling of {@link ClosedByInterruptException}s. When
 * a commit group is aborted, the {@link Thread}s for the tasks in that commit
 * group are interrupted. If a task was in the midst of an IO operation on a
 * {@link Channel} then the channel will be asynchronously closed by the JDK.
 * <p>
 * Since we use a {@link FileChannel} to access the backing store, this means
 * that we need to transparently re-open the channel so that we can continue
 * operations on the store.
 * <p>
 * Note: The tests in this class require a {@link BufferStrategy} that is backed
 * by stable storage (i.e., by a {@link FileChannel}). They doubtless will NOT
 * work for a mapped file since you have no control over when the file is
 * unmapped under Java.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractInterruptsTestCase extends AbstractRawStoreTestCase {

    /**
     * 
     */
    public AbstractInterruptsTestCase() {
        super();
    }

    /**
     * @param name
     */
    public AbstractInterruptsTestCase(String name) {
        super(name);
    }

    /**
     * Runs {@link #doChannelOpenAfterInterrupt()} N times.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_channelOpenAfterInterrupt()
            throws InterruptedException, ExecutionException {
        
        for(int i=0; i<10; i++) {
            
            doChannelOpenAfterInterrupt();
            
        }
        
    }

    /**
     * Verifies that the backing {@link Channel} is re-opened after an abort.
     * The test is designed to provoke a {@link ClosedByInterruptException} that
     * causes the backing channel to be closed. This in turn causes an abort to
     * discard the commit group. Afterwards we verify that the {@link Channel}
     * has been re-opened.
     * 
     * @param properties
     * 
     * @throws InterruptedException
     */
    public void doChannelOpenAfterInterrupt() throws InterruptedException {

        final IRawStore store = getStore();
        
        try {

        // Note: This test requires a journal backed by stable storage.
        
        if(store.isStable() && store instanceof IJournal) {

            final Journal journal = (Journal)store;
            
            final String[] resource = new String[]{"foo"};//,"bar","baz"};
            
            // register the indices.
            for (int i = 0; i < resource.length; i++) {
             
                journal.registerIndex(resource[i]);
                
            }

            // and commit (but NOT using the writeService).
            journal.commit();

            // verify counters.

            assertEquals("abortCount", 0,
                    journal.getConcurrencyManager().writeService
                            .getAbortCount());
            
            assertEquals("commitCount", 0,
                    journal.getConcurrencyManager().writeService
                            .getGroupCommitCount());

            /*
             * Submit a task that waits for 10 seconds or until interrupted. It
             * will be in the same commit group as the next task since it is
             * just waiting around.
             * 
             * Note: This MUST run on the write service so that it is the commit
             * group. However it MUST NOT declare a lock on the named index or
             * the 'interrupt' task will not get to execute. Also, do NOT
             * "get()" this task since that will block and the 'interrupt' task
             * will not run.
             */
            journal.submit(new AbstractTask(journal,ITx.UNISOLATED,new String[]{}){

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
                
                journal.submit(
                        new InterruptMyselfTask(journal, ITx.UNISOLATED,
                                resource[0])).get();
                
                fail("Not expecting success");
                
            } catch (ExecutionException ex) {
                
                log.warn(ex, ex);
                
                assertTrue(isInnerCause(ex, ClosedByInterruptException.class)
                        || isInnerCause(ex, InterruptedException.class));
                
            }

            // Wait until the write service either commits or aborts.
            
            log.warn("Waiting for the write service to commit or abort");
            
            while (journal.getConcurrencyManager().writeService.getAbortCount() == 0
                    && journal.getConcurrencyManager().writeService
                            .getGroupCommitCount() == 0) {
                
                Thread.sleep(10);
                
            }
            
            // did abort.
            assertEquals("abortCount", 1,
                    journal.getConcurrencyManager().writeService
                            .getAbortCount());

            // did not commit.
            assertEquals("commitCount", 0,
                    journal.getConcurrencyManager().writeService.getGroupCommitCount());
            
            /*
             * write on the store and flush the store to disk in order to provoke an
             * exception if the channel is still closed.
             */

            journal.write(ByteBuffer.wrap(new byte[]{1,2,3}));
            
            journal.force(true);
            
        }

        } finally {

            store.destroy();
            
        }

    }
    
    /**
     * Task interrupts itself and then forces an IO operation on the
     * {@link FileChannel} for the journal in order to provoke a
     * {@link ClosedByInterruptException}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class InterruptMyselfTask extends AbstractTask {

        protected InterruptMyselfTask(IConcurrencyManager concurrencyManager,
                long startTime, String resource) {

            super(concurrencyManager, startTime, resource);
            
        }

        protected Object doTask() throws Exception {
            
            // Get the live version of the named index.
            final BTree ndx = (BTree) getIndex(getOnlyResource());

            // write on the index.
            ndx.insert(new byte[]{},new byte[]{});

            /*
             * Now provoke a ClosedByInterruptException.
             */
            
            // interrupt ourselves.
            Thread.currentThread().interrupt();

            try {
            
            // force flush of index to store
            ndx.writeCheckpoint();
            
            // force flush of store to disk.
            getJournal().force(true);

            } catch(Exception ex) {

                assertTrue(isInnerCause(ex, ClosedByInterruptException.class));

                log.info("Provoked expected root cause exception: " + ex);
                
                throw ex;
                
            } catch(Throwable t) {
                
                throw new RuntimeException("Not expecting: "+t, t);
                
            }
            
            throw new AssertionError("Not expecting to access index.");
            
        }
        
    }

    /**
     * A simple test verifies that a read will transparently re-open the backing
     * {@link FileChannel} after a {@link ClosedByInterruptException}.
     * <p>
     * The test uses the {@link IRawStore} API. It writes an initial record on
     * the store and commits. It then interrupts the main thread and then
     * performs another low level write on the store. The store is then forced
     * to disk to ensure that a {@link ClosedByInterruptException} is triggered,
     * so this second record is never written.
     * <p>
     * Once the {@link ClosedByInterruptException} has been triggered, we then
     * attempt to re-read the 1st record, which was made restart safe by the
     * commit.
     * <p>
     * Note: This test is only for {@link IDiskBasedStrategy} implementations.
     */
    public void test_reopenAfterInterrupt() {
        
        final IRawStore store = getStore();

        try {
        
        if (store.isStable()) {

            final ByteBuffer rec1 = getRandomData();

            final long addr1 = store.write(rec1);

            if(store instanceof IAtomicStore) {
                
                assertNotSame(0L, ((IAtomicStore)store).commit());
                
            }

            try {

                Thread.currentThread().interrupt();

                store.write(getRandomData());
                
                store.force(true);

                fail("Expecting: " + ClosedByInterruptException.class);

            } catch (Throwable t) {

                assertTrue(isInnerCause(t, ClosedByInterruptException.class));

                // clear the interrupt.
                assertTrue(Thread.interrupted());
                
            }

            final ByteBuffer actual = store.read(addr1);
            
            AbstractRawStoreTestCase.assertEquals(rec1.array(),actual);
            
        }

        } finally {

            store.destroy();
            
        }
        
    }
    
    /**
     * A simple test verifies that a read will transparently re-open the backing
     * {@link FileChannel} after a {@link ClosedByInterruptException}.
     * <p>
     * The test uses the {@link IRawStore} API. It writes an initial record on
     * but does NOT force the store to the backing file. It then interrupts the
     * main thread and issues a request to force the store to disk. This request
     * triggers a {@link ClosedByInterruptException}. At this point nothing has
     * been written on the file.
     * <p>
     * Once the {@link ClosedByInterruptException} has been triggered, we then
     * attempt to re-read the record that was written. If the store buffers
     * writes then this operation will succeed.
     * <p>
     * Note: Both the {@link DirectBufferStrategy} and the
     * {@link DiskOnlyStrategy} buffer writes, so both should pass this test.
     * <p>
     * Note: This test is only for {@link IDiskBasedStrategy} implementations.
     */
    public void test_reopenAfterInterrupt_checkWriteBuffer() {
        
        final IRawStore store = getStore();

        try {
        
        if (store.isStable()) {

            final ByteBuffer rec1 = getRandomData();

            final long addr1 = store.write(rec1);

            try {

                Thread.currentThread().interrupt();
                
                store.force(true);

                fail("Expecting: " + ClosedByInterruptException.class);

            } catch (Throwable t) {

                assertTrue(isInnerCause(t, ClosedByInterruptException.class));

                // clear the interrupt.
                assertTrue(Thread.interrupted());

            }

            final ByteBuffer actual = store.read(addr1);
            
            AbstractRawStoreTestCase.assertEquals(rec1.array(),actual);
            
        }
        
        } finally {

            store.destroy();
            
        }
        
    }
    
// /**
// * Runs the test a bunch of times to see if it fails.
// *
// * @param args
// * @throws InterruptedException
// * @throws ExecutionException
// */
// public static void main(String[] args) throws InterruptedException,
// ExecutionException {
//        
// final int limit = 100;
//        
//        for (int i = 0; i < limit; i++) {
//
//            Properties properties = new Properties();
//
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                    .toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT, "true");
//
//            doHandleClosedByInterruptException(new Journal(properties));
//
//        }
//        
//        System.err.println("Ok");
//        
//    }

}
