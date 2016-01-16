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
package com.bigdata.ha.halog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.Bytes;
import com.bigdata.util.DaemonThreadFactory;

/**
 * Test suite for {@link HALogWriter} and {@link HALogReader}.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn Cutcher</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHALogWriter extends TestCase2 {

    private Random r;
    private File logdir;
    private ExecutorService executorService;

    @Override
    protected void setUp() throws Exception {

        super.setUp();

        // create temporary file for the test.
        logdir = File.createTempFile(getClass().getSimpleName(), "halogdir");

        // delete temp file. will recreate as a directory.
        if (!logdir.delete())
            throw new IOException("Could not remove: file=" + logdir);
        
        // re-create as a directory.
        if (!logdir.mkdirs())
            throw new IOException("Could not create: dir=" + logdir);

        r = new Random();
        
        executorService = Executors.newCachedThreadPool(DaemonThreadFactory
                .defaultThreadFactory());
        
    }
    
    @Override
    protected void tearDown() throws Exception {

        super.tearDown();

        r = null;

        if (logdir != null && logdir.exists()) {

            recursiveDelete(logdir);

        }

        if (executorService != null) {

            executorService.shutdownNow();

            executorService = null;

        }

    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. 
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles();

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }
    
	/*
	 * Need to mock up some valid rootblocks
	 * 
	 * RootBlockView(// final boolean rootBlock0, final int offsetBits, final
	 * long nextOffset, final long firstCommitTime, final long lastCommitTime,
	 * final long commitCounter, final long commitRecordAddr, final long
	 * commitRecordIndexAddr, final UUID uuid, final long blockSequence, //
	 * VERSION3 final long quorumToken, // VERSION2 final long metaStartAddr, //
	 * VERSION1 final long metaBitsAddr, // VERSION1 final StoreTypeEnum
	 * storeTypeEnum, // VERSION1 final long createTime, final long closeTime,
	 * final int version, final ChecksumUtility checker)
	 */
	private static IRootBlockView openRBV(final StoreTypeEnum st) {
		return new RootBlockView(
				//
				true /* rb0 */, 0, 0, 0 /* commitTime */, 0,
				0 /* commitCounter */, 0, 0, new UUID(1, 2),
				0, // VERSION3
				23, // NOQUORUM
				0, // VERSION1
				0, // VERSION1
				st, // storetype
				System.currentTimeMillis(), 0, RootBlockView.currentVersion,
				ChecksumUtility.getCHK());
	}

	private static IRootBlockView closeRBV(final IRootBlockView rbv) {
		return new RootBlockView(
				//
				!rbv.isRootBlock0(), 0, 0,
				System.currentTimeMillis() /* commitTime */, 0, rbv
						.getCommitCounter() + 1 /* commitCounter */, 100,
				100, // non-zero commit records
				rbv.getUUID(),
				0, // VERSION3
				rbv.getQuorumToken(),
				0, // VERSION1
				0, // VERSION1
				rbv.getStoreType(), // storetype
				rbv.getCreateTime(), System.currentTimeMillis(),
				RootBlockView.currentVersion, ChecksumUtility.getCHK());
	}

	private ByteBuffer randomData(final int sze) {

	    final byte[] buf = new byte[sze];
		
	    r.nextBytes(buf);

		return ByteBuffer.wrap(buf, 0, sze);
		
	}

    /**
     * Simple writelog test, open file, write data and commit.
     */
    public void testSimpleRWWriter() throws FileNotFoundException, IOException,
            InterruptedException {

        final HALogWriter writer = new HALogWriter(logdir);

        try {

            final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

            assertEquals(StoreTypeEnum.RW, rbv.getStoreType());

            writer.createLog(rbv);

            int sequence = 0;

            final ByteBuffer data = randomData(2000);

            final UUID storeUUID = UUID.randomUUID();

            final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                    rbv.getCommitCounter(), rbv.getFirstCommitTime(), sequence,
                    data.limit()/* size */, ChecksumUtility.getCHK().checksum(
                            data), rbv.getStoreType(), rbv.getQuorumToken(),
                    1000/* fileExtent */, 0/* firstOffset */);

            writer.writeOnHALog(msg, data);

            writer.closeHALog(closeRBV(rbv));

        } finally {

            writer.disableHALog();

        }

        // Read all files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

	}

	/**
	 * Simple WriteReader, no concurrency, confirms non-delayed responses.
	 */
	public void testSimpleRWWriterReader() throws FileNotFoundException,
			IOException, InterruptedException {

		final HALogWriter writer = new HALogWriter(logdir);
		
		try {

		    // The opening root block.
            final IRootBlockView openRB = openRBV(StoreTypeEnum.RW);

            assertEquals(StoreTypeEnum.RW, openRB.getStoreType());

            {
                // should not be able to open the reader yet.
                try {
                    writer.getReader(openRB.getCommitCounter() + 1);
                } catch (FileNotFoundException ex) {
                    // Ignore expected exception.
                    if (log.isInfoEnabled())
                        log.info("Ignoring expected exception: " + ex);
                }

            }

            // writer is not open.
            assertFalse(writer.isHALogOpen());

            // create HALog file.
            writer.createLog(openRB);

            {

                // writer is open.
                assertTrue(writer.isHALogOpen());
             
                // should be able to open the reader for that log now.
                final IHALogReader reader = writer.getReader(openRB
                        .getCommitCounter() + 1);

                try {

                    // This is the "live" HALog.
                    assertTrue(reader.isLive());

                    // The reader is open.
                    assertTrue(reader.isOpen());

                    // The HALog is logically empty.
                    // assertTrue(reader.isEmpty());

                    /*
                     * Note: Don't do this here. The method will block for the
                     * live HALog until the file is closed (sealed with the
                     * closing root block) or destroyed.
                     */
                    // assertTrue(reader.hasMoreBuffers());

                    // close the reader. should not close the writer.
                    reader.close();

                    // the reader is closed.
                    assertFalse(reader.isOpen());

                    // once closed, this method should return immediately.
                    assertFalse(reader.hasMoreBuffers());

                    // the writer is still open.
                    assertTrue(writer.isHALogOpen());

                    // double-close the reader. should be ignored.
                    reader.close();

                    // the reader is closed.
                    assertFalse(reader.isOpen());

                    // the writer should *still* be open.
                    assertTrue(writer.isHALogOpen());

                } finally {
                    
                    if(reader.isOpen()) {
                        
                        reader.close();
                        
                    }
                    
                }
                
            }

            /*
             * Verify that we can open two distinct readers on the same live
             * HALog and that closing one does not close the other and does not
             * close the writer.
             */
            {
                
                final IHALogReader r1 = writer.getReader(openRB.getCommitCounter() + 1);
                final IHALogReader r2 = writer.getReader(openRB.getCommitCounter() + 1);
                
                assertTrue(r1.isOpen());
                assertTrue(r2.isOpen());

                // close one reader.
                r1.close();
               
                // one reader is closed, the other is open.
                assertFalse(r1.isOpen());
                assertTrue(r2.isOpen());

                // the writer should *still* be open.
                assertTrue(writer.isHALogOpen());

                // close the other reader.
                r2.close();

                // Verify both are now closed.
                assertFalse(r2.isOpen());
                assertFalse(r2.isOpen());
                
                // the writer should *still* be open.
                assertTrue(writer.isHALogOpen());

            }
            
            int sequence = 0;

            final ByteBuffer data = randomData(2000);

            final UUID storeUUID = UUID.randomUUID();

            final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                    openRB.getCommitCounter(), openRB.getFirstCommitTime(), sequence,
                    data.limit()/* size */, ChecksumUtility.getCHK().checksum(
                            data), openRB.getStoreType(), openRB.getQuorumToken(),
                    1000/* fileExtent */, 0/* firstOffset */);

            // write a message on the HALog.
            writer.writeOnHALog(msg, data);

            // should be able to open the reader for that log now.
            final IHALogReader reader = writer
                    .getReader(openRB.getCommitCounter() + 1);

            assertTrue(reader.hasMoreBuffers());

            {

                // Allocate heap byte buffer for the reader.
                final ByteBuffer rbuf = ByteBuffer
                        .allocate(DirectBufferPool.INSTANCE.getBufferCapacity());

                final IHAWriteMessage rmsg = reader.processNextBuffer(rbuf);

                assertEquals(rmsg.getSize(), msg.getSize());
                
            }

            // commit the log file (write the closing root block).
            writer.closeHALog(closeRBV(openRB));

            /*
             * The writer should have closed the file, so the reader should
             * return immediately to report no more buffers.
             */
            assertFalse(reader.hasMoreBuffers());

        } finally {

		    writer.disableHALog();
		    
		}
		
        // Read all HALog files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

	}

	/**
	 * SimpleWriter writes a number of log files with a set of messages in each
	 */
	private class SimpleWriter implements Callable<Void> {

		private IRootBlockView openRB;
		private final HALogWriter writer;
		private final int count;

        /**
         * 
         * @param openRB
         *            The opening root block.
         * @param writer
         *            The {@link HALogWriter}.
         * @param count
         *            The HALog files to write. Each will have a random #of
         *            records.
         */
        SimpleWriter(final IRootBlockView openRB, final HALogWriter writer,
                final int count) {

            this.openRB = openRB;
			this.writer = writer;
			this.count = count;

		}
		
		@Override
		public Void call() throws Exception {

		    final UUID storeUUID = UUID.randomUUID();
            final long fileExtent = 1000; // NB: ignored for test.
            final long firstOffset = 0; // NB: ignored for test.
            
            // Note: Thread Local!  Can not be passed in by the caller.
            final ChecksumUtility checker = ChecksumUtility.getCHK();
			
            for (int i = 0; i < count; i++) {

                // Min of 1 message. Max of r.nextInt().
                final long nmessages = r.nextInt(100) + 1;
                
                for (long sequence = 0; sequence < nmessages; sequence++) {
                
                    // add delay to write thread to test reader waiting
                    Thread.sleep(10);

                    // Use random data of random length.
                    final int size = r.nextInt(4 * Bytes.kilobyte32) + 1;

                    final ByteBuffer data = randomData(size);

                    final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                            openRB.getCommitCounter(),
                            openRB.getLastCommitTime(), sequence, size,
                            checker.checksum(data), openRB.getStoreType(),
                            openRB.getQuorumToken(), fileExtent, firstOffset);

                    writer.writeOnHALog(msg, data);

                }

                if (log.isInfoEnabled())
                    log.info("Cycling HALog after " + nmessages + " records");

                // close log.
                writer.closeHALog(openRB = closeRBV(openRB));
                
                // open new log.
                writer.createLog(openRB);

            } // next HALog file.

            // Close the last HALog.
            writer.closeHALog(openRB = closeRBV(openRB));

            // Done.
            return null;

        }

    } // class SimpleWriter.

    /**
     * Reader consumes an HALog file. The file must exist before you start
     * running the {@link ReaderTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
	private static class ReaderTask implements Callable<Long> {

	    private final long commitCounter;
	    private final HALogWriter writer;
        private final Future<Void> wf;

        /**
         * 
         * @param commitCounter
         *            The commit counter that identifies the closing commit
         *            point for the HALog file to be read.
         * @param writer
         *            The {@link HALogWriter}.
         * @param wf
         *            The {@link Future} for the {@link HALogWriter}. This is
         *            used to monitor for an error in the writer so the reader
         *            does not block the test from completing (or failing).
         */
        public ReaderTask(final long commitCounter, final HALogWriter writer,
                final Future<Void> wf) {

            this.commitCounter = commitCounter;

            this.writer = writer;

            this.wf = wf;

	    }

        /** Test future if done. Throws exception if writer fails. */
        private void checkWriterFuture() throws InterruptedException,
                ExecutionException {

            if (wf.isDone()) {

                wf.get();

            }

        }

	    /**
	     *
	     * @return The #of {@link IHAMessage}s read from the file.
	     */
        @Override
        public Long call() throws Exception {

            // Allocate a heap ByteBuffer
            final ByteBuffer rbuf = ByteBuffer
                    .allocate(DirectBufferPool.INSTANCE.getBufferCapacity());

            // Note: Throws FileNotFoundException if does not exist.
            final IHALogReader reader = writer.getReader(commitCounter);

            try {

                assertNotNull(reader);

                long nread = 0L;
                
                while (reader.hasMoreBuffers()) {

                    checkWriterFuture();

                    final IHAWriteMessage rmsg = reader.processNextBuffer(rbuf);

                    if (log.isDebugEnabled())
                        log.debug("Read message: " + rmsg.getSequence()
                                + ", size: " + rmsg.getSize());

                    assertEquals(nread, rmsg.getSequence());

                    nread++;

                    checkWriterFuture();

                }
                
                return nread;
                
            } finally {

                /*
                 * Note: This should not throw an IOException.
                 * 
                 * Note: It it does throw an IOException, then it can also be
                 * masking an error in the try{} above. Diagnose both if you get
                 * anything thrown out of here.
                 */
 
                reader.close();
                
            }
            
        }

	}
	
	/**
	 * While a writer thread writes a number of HALogs, readers are opened
	 * to process them.
	 * 
	 * @throws Exception 
	 */
    public void testConcurrentRWWriterReader() throws Exception {

		final HALogWriter writer = new HALogWriter(logdir);
		
        final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

        assertEquals(StoreTypeEnum.RW, rbv.getStoreType());

		writer.createLog(rbv);

        // The #of HALog files to write. If GT 1000, then more than one
        // subdirectory worth of files will be written.
        final int nfiles = 100 + r.nextInt(1000);

        // Start the writer.
        final Future<Void> wf = executorService.submit(new SimpleWriter(rbv,
                writer, nfiles));

        try {

            /*
             * Now keep on opening readers for "current file" while writer
             * continues.
             * 
             * Note: The writer will write multiple files. For each file that it
             * writes, we run the reader until it is done, then we open a new
             * reader on the next HALog file.
             */
            for (long commitCounter = 1L; commitCounter <= nfiles; commitCounter++) {

                /*
                 * Note: We need to spin here in case the reader tries to open
                 * the HALog for reading before the writer has created the HALog
                 * for that commit point. This can be done by monitoring the
                 * writer or the file system.
                 */
                final File file = CommitCounterUtility.getCommitCounterFile(
                        logdir, commitCounter, IHALogReader.HA_LOG_EXT);

                while (!file.exists()) {

                    if (wf.isDone()) {
                        // Check writer for errors.
                        wf.get();
                    }

                    if (log.isInfoEnabled())
                        log.info("Blocked waiting on writer: commitCounter="
                                + commitCounter + ", file=" + file);
                    
                    // Wait for the file.
                    Thread.sleep(100/* ms */);

                }

                /*
                 * Open and read the next HALog file, blocking until all data
                 * has been read from that file.
                 */
                new ReaderTask(commitCounter, writer, wf).call();

            }

            // Check writer for errors. There should not be any.
            wf.get();
            
        } finally {

            wf.cancel(true/* mayInterruptIfRunning */);
            
        }

        // for sanity, let's run through the standard reader
        HALogReader.main(new String[] { logdir.toString() });

	}

    /**
     * A unit test where the reader is blocked awaiting more input in
     * {@link IHALogReader#hasMoreBuffers()} on the live HALog. The writer is
     * closed. The reader should immediately notice this event and return
     * <code>false</code>.
     */
    public void test_closeLiveLogWithOpenReader() throws IOException,
            InterruptedException, ExecutionException {
     
        final HALogWriter writer = new HALogWriter(logdir);

        try {

            final IRootBlockView openRB = openRBV(StoreTypeEnum.RW);

            assertEquals(StoreTypeEnum.RW, openRB.getStoreType());

            writer.createLog(openRB);

            final IHALogReader reader = writer.getReader(openRB
                    .getCommitCounter() + 1);

            try {

                // Allocate a heap ByteBuffer
                final ByteBuffer rbuf = ByteBuffer
                        .allocate(DirectBufferPool.INSTANCE.getBufferCapacity());

                int sequence = 0;

                final ByteBuffer data = randomData(2000);

                final UUID storeUUID = UUID.randomUUID();

                final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                        openRB.getCommitCounter(), openRB.getFirstCommitTime(),
                        sequence, data.limit()/* size */, ChecksumUtility
                                .getCHK().checksum(data),
                        openRB.getStoreType(), openRB.getQuorumToken(),
                        1000/* fileExtent */, 0/* firstOffset */);

                writer.writeOnHALog(msg, data);

                final Future<Void> f = executorService
                        .submit(new Callable<Void>() {
                            public Void call() throws Exception {
                                // should be immediately true.
                                assertTrue(reader.hasMoreBuffers());
                                // read data into reader's buffer.
                                reader.processNextBuffer(rbuf);
                                // should block until writer is closed.
                                assertFalse(reader.hasMoreBuffers());
                                // done - success.
                                return (Void) null;
                            }
                        });

                // Make sure the Futuer is blocked.
                try {
                    f.get(500, TimeUnit.MILLISECONDS);
                    fail("Reader did not block");
                } catch (TimeoutException ex) {
                    // ignore expected exception
                }

                writer.closeHALog(closeRBV(openRB));

                // Block and wait for the future. Verify no errors.
                f.get();

            } finally {

                reader.close();

            }

        } finally {

            writer.disableHALog();

        }

        // Read all files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

    }

    /**
     * A unit test where the reader is blocked awaiting more input in
     * {@link IHALogReader#hasMoreBuffers()} on the live HALog. The writer is
     * {@link HALogWriter#disableHALog() disabled}. The reader should
     * immediately notice this event and return <code>false</code>.
     */
    public void test_disableLiveLogWithOpenReader() throws IOException,
            InterruptedException, ExecutionException {

        final HALogWriter writer = new HALogWriter(logdir);

        try {

            final IRootBlockView openRB = openRBV(StoreTypeEnum.RW);

            assertEquals(StoreTypeEnum.RW, openRB.getStoreType());

            writer.createLog(openRB);

            final IHALogReader reader = writer.getReader(openRB
                    .getCommitCounter() + 1);

            try {

                // Allocate a heap ByteBuffer
                final ByteBuffer rbuf = ByteBuffer
                        .allocate(DirectBufferPool.INSTANCE.getBufferCapacity());

                int sequence = 0;

                final ByteBuffer data = randomData(2000);

                final UUID storeUUID = UUID.randomUUID();

                final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                        openRB.getCommitCounter(), openRB.getFirstCommitTime(),
                        sequence, data.limit()/* size */, ChecksumUtility
                                .getCHK().checksum(data),
                        openRB.getStoreType(), openRB.getQuorumToken(),
                        1000/* fileExtent */, 0/* firstOffset */);

                writer.writeOnHALog(msg, data);

                final Future<Void> f = executorService
                        .submit(new Callable<Void>() {
                            public Void call() throws Exception {
                                // should be immediately true.
                                assertTrue(reader.hasMoreBuffers());
                                // read data into reader's buffer.
                                reader.processNextBuffer(rbuf);
                                // should block until writer is closed.
                                assertFalse(reader.hasMoreBuffers());
                                // done - success.
                                return (Void) null;
                            }
                        });

                // Make sure the Futuer is blocked.
                try {
                    f.get(500, TimeUnit.MILLISECONDS);
                    fail("Reader did not block");
                } catch (TimeoutException ex) {
                    // ignore expected exception
                }

                writer.disableHALog();

                // Block and wait for the future. Verify no errors.
                f.get();

            } finally {

                reader.close();

            }

        } finally {

            writer.disableHALog();

        }

        // Read all files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

    }

    /**
     * Unit test verifies that each open of an {@link IHALogReader} is distinct
     * and the an {@link IHALogReader#close()} will not close the backing
     * channel for a different reader instance that is reading from the same
     * HALog file. This version of the test is for a historical (non-live) HALog
     * file. The case for the live HALog file is tested by
     * {@link #testSimpleRWWriterReader()}.
     */
    public void test_doubleOpen_close_historicalHALog() throws Exception {
        
        IHALogReader r1 = null, r2 = null;
        
        final HALogWriter writer = new HALogWriter(logdir);

        try {

            /*
             * Generate and close (seal with a closing root block) an HALog
             * file.
             */
            final IRootBlockView openRB = openRBV(StoreTypeEnum.RW);

            {
                
                assertEquals(StoreTypeEnum.RW, openRB.getStoreType());

                writer.createLog(openRB);

                int sequence = 0;

                final ByteBuffer data = randomData(2000);

                final UUID storeUUID = UUID.randomUUID();

                final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                        openRB.getCommitCounter(), openRB.getFirstCommitTime(),
                        sequence, data.limit()/* size */, ChecksumUtility
                                .getCHK().checksum(data),
                        openRB.getStoreType(), openRB.getQuorumToken(),
                        1000/* fileExtent */, 0/* firstOffset */);

                writer.writeOnHALog(msg, data);

                writer.closeHALog(closeRBV(openRB));

            }

            /*
             * The HALog file is now closed.
             * 
             * Setup two readers on that HALog file.
             */
            
            r1 = writer.getReader(openRB.getCommitCounter() + 1);

            assertFalse(r1.isLive());
            assertTrue(r1.isOpen());
            assertFalse(r1.isEmpty());
            assertTrue(r1.hasMoreBuffers());

            r2 = writer.getReader(openRB.getCommitCounter() + 1);

            assertFalse(r2.isLive());
            assertTrue(r2.isOpen());
            assertFalse(r2.isEmpty());
            assertTrue(r2.hasMoreBuffers());
            
            /*
             * Close one of the readers and make sure that the other reader
             * remains open.
             */
            
            // close [r1].
            r1.close();
            assertFalse(r1.isLive());
            assertFalse(r1.isOpen());
            assertFalse(r1.isEmpty());
            assertFalse(r1.hasMoreBuffers());
            
            // verify [r2] remains open.
            assertFalse(r2.isLive());
            assertTrue(r2.isOpen());
            assertFalse(r2.isEmpty());
            assertTrue(r2.hasMoreBuffers());            
            
            /*
             * Now use the 2nd reader to read the data to make sure that the
             * IHALogReader is really open and functional.
             */
            try {
                
                // Allocate a heap ByteBuffer
                final ByteBuffer rbuf = ByteBuffer
                        .allocate(DirectBufferPool.INSTANCE.getBufferCapacity());

                while (r2.hasMoreBuffers()) {
                    // read data into reader's buffer.
                    r2.processNextBuffer(rbuf);
                }
            
            } finally {
                
                r2.close();
                
            }

            assertFalse(r2.isLive());
            assertFalse(r2.isOpen());
            assertFalse(r2.isEmpty());
            assertFalse(r2.hasMoreBuffers());
            
        } finally {

            writer.disableHALog();

            if (r1 != null && r1.isOpen()) {
                r1.close();
                r1 = null;
            }
            
            if (r2 != null && r2.isOpen()) {
                r2.close();
                r2 = null;
            }
            
        }

        // Read all files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

    }

    /**
     * Unit test for an open file leak for a historical log reader.
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/678#comment:4"
     *      > DGC Thread Leak: sendHALogForWriteSet() </a>
     */
    public void test_fileLeak_historicalHALog() throws Exception {

        /*
         * This should be more than the #of open file handles that are supported
         * by the OS platform / configured limits. Of course, some platforms do
         * not have limits in which case this test can not fail on those
         * platforms, but you could still use something like "lsof" or a heap
         * dump / profiler to look for leaked file handles.
         */
        final int MAX_OPEN_FILE_HANDLES = 10000;
        
        final HALogWriter writer = new HALogWriter(logdir);

        try {

            /*
             * Generate and close (seal with a closing root block) an HALog
             * file.
             */
            final IRootBlockView openRB = openRBV(StoreTypeEnum.RW);

            {
                
                assertEquals(StoreTypeEnum.RW, openRB.getStoreType());

                writer.createLog(openRB);

                int sequence = 0;

                final ByteBuffer data = randomData(2000);

                final UUID storeUUID = UUID.randomUUID();

                final IHAWriteMessage msg = new HAWriteMessage(storeUUID,
                        openRB.getCommitCounter(), openRB.getFirstCommitTime(),
                        sequence, data.limit()/* size */, ChecksumUtility
                                .getCHK().checksum(data),
                        openRB.getStoreType(), openRB.getQuorumToken(),
                        1000/* fileExtent */, 0/* firstOffset */);

                writer.writeOnHALog(msg, data);

                writer.closeHALog(closeRBV(openRB));

            }

            /*
             * The HALog file is now closed.
             * 
             * Setup a reader on that HALog file. This reader will stay open. We
             * then open and close and second reader a bunch of times. These
             * readers should be completely distinct and use distinct file
             * handles to read on the same file. Thus the #of open file handles
             * should not grow over time.
             */

            final IHALogReader r1 = writer
                    .getReader(openRB.getCommitCounter() + 1);

            try {

                assertFalse(r1.isLive());
                assertTrue(r1.isOpen());
                assertFalse(r1.isEmpty());
                assertTrue(r1.hasMoreBuffers());

                for (int i = 0; i < MAX_OPEN_FILE_HANDLES; i++) {

                    final IHALogReader r2 = writer.getReader(openRB
                            .getCommitCounter() + 1);

                    assertFalse(r2.isLive());
                    assertTrue(r2.isOpen());
                    assertFalse(r2.isEmpty());
                    assertTrue(r2.hasMoreBuffers());

                    /*
                     * Now use the 2nd reader to read the data to make sure that
                     * the IHALogReader is really open and functional.
                     */
                    try {

                        // Allocate a heap ByteBuffer
                        final ByteBuffer rbuf = ByteBuffer
                                .allocate(DirectBufferPool.INSTANCE
                                        .getBufferCapacity());

                        while (r2.hasMoreBuffers()) {

                            // read data into reader's buffer.
                            r2.processNextBuffer(rbuf);

                        }

                    } finally {

                        r2.close();

                    }

                    assertFalse(r2.isLive());
                    assertFalse(r2.isOpen());
                    assertFalse(r2.isEmpty());
                    assertFalse(r2.hasMoreBuffers());

                }

                // close [r1].
                r1.close();
                assertFalse(r1.isLive());
                assertFalse(r1.isOpen());
                assertFalse(r1.isEmpty());
                assertFalse(r1.hasMoreBuffers());

            } finally {
                
                if (r1.isOpen())
                    r1.close();

            }
            
        } finally {

            writer.disableHALog();

        }

        // Read all files in the test directory.
        HALogReader.main(new String[] { logdir.toString() });

    }

}
