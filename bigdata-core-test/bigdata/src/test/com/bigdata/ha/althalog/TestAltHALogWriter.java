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

package com.bigdata.ha.althalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;

import junit.framework.TestCase;

public class TestAltHALogWriter extends TestCase {

	private static final Logger log = Logger
			.getLogger(TestAltHALogWriter.class);

	final File m_logdir = new File("/tmp/halogdir");
	// final File m_logdir = new File("/Volumes/SSDData/tmp/halogdir");

	protected void setUp() {
		try {
			m_logdir.mkdirs();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void tearDown() {
		// clear directory
		final File[] files = m_logdir.listFiles();
		for (int i = 0; i < files.length; i++) {
			try {
				files[i].delete();
			} catch (Exception e) {
				// ignore
			}
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
	@SuppressWarnings("deprecation")
	private IRootBlockView openRBV(final StoreTypeEnum st) {
		return new RootBlockView(
				//
				true /* rb0 */, 0, 0, 
				0 /* commitTime */, 0,
				0 /* commitCounter */, 0, 0, new UUID(1, 2),
				0, // VERSION3
				23, // NOQUORUM
				0, // VERSION1
				0, // VERSION1
				st, // storetype
				System.currentTimeMillis(), 0, RootBlockView.currentVersion,
				ChecksumUtility.getCHK());
	}

	@SuppressWarnings("deprecation")
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

	final static Random r = new Random();

	static ByteBuffer randomData(final int sze) {
		byte[] buf = new byte[sze];
		r.nextBytes(buf);

		return ByteBuffer.wrap(buf, 0, sze);
	}

	/**
	 * Simple writelog test, open file, write data and commit.
	 */
	@SuppressWarnings("deprecation")
	public void testSimpleRWWriter() throws FileNotFoundException, IOException {

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final IHALogWriter writer = manager.createLog(rbv).getWriter();

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

        final UUID storeUUID = UUID.randomUUID();

        final IHAWriteMessage msg = new HAWriteMessage(storeUUID, rbv.getCommitCounter(), rbv
				.getFirstCommitTime(), sequence, data.limit(), checker
				.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
				1000, 0);

		writer.write(msg, data);

		writer.close(closeRBV(rbv));

		// for sanity, let's run through the standard reader
		try {
			HALogManager.main(new String[] { m_logdir.getAbsolutePath() });
		} catch (InterruptedException e) {
			// NOP
		}
	}

	/**
	 * Simple WriteReader, no concurrency, confirms non-delayed responses.
	 */
	@SuppressWarnings("deprecation")
	public void testSimpleRWWriterReader() throws FileNotFoundException,
			IOException {

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final HALogFile logfile = manager.createLog(rbv);
		final IHALogWriter writer = logfile.getWriter();

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

        final UUID storeUUID = UUID.randomUUID();

		final IHAWriteMessage msg = new HAWriteMessage(storeUUID, rbv.getCommitCounter(), rbv
				.getFirstCommitTime(), sequence, data.limit(), checker
				.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
				1000, 0);

		writer.write(msg, data);

		final IHALogReader reader = logfile.getReader();

		assertTrue(reader.hasMoreBuffers());

		ByteBuffer rbuf = ByteBuffer.allocate(1 * 1024 * 1024); // 1 mb
		IHAWriteMessage rmsg = reader.processNextBuffer(rbuf);

		assertTrue(rmsg.getSize() == msg.getSize());

		// commit the log file
		writer.close(closeRBV(rbv));

		// the writer should have closed the file, so the reader should return
		// immediately to report no more buffers
		assertFalse(reader.hasMoreBuffers());
		
		assertTrue(logfile.isOpen());

		reader.close();
		
		assertFalse(logfile.isOpen());

		// for sanity, let's run through the standard reader
		try {
			HALogManager.main(new String[] { m_logdir.getAbsolutePath() });
		} catch (InterruptedException e) {
			// NOP
		}
	}
	
	@SuppressWarnings("deprecation")
	public void testDisableLogFile() throws IOException {
		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final IHALogWriter writer = manager.createLog(rbv).getWriter();

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

        final UUID storeUUID = UUID.randomUUID();

        final IHAWriteMessage msg = new HAWriteMessage(storeUUID, rbv.getCommitCounter(), rbv
				.getFirstCommitTime(), sequence, data.limit(), checker
				.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
				1000, 0);

		writer.write(msg, data);

		// disable current writer
		manager.disable();
		
		try {

		    final IHAWriteMessage msg2 = new HAWriteMessage(storeUUID, rbv.getCommitCounter(), rbv
					.getFirstCommitTime(), sequence+1, data.limit(), checker
					.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
					1000, 0);
	
			writer.write(msg2, data);
			fail("The file should have been disabled!");
		} catch (IllegalStateException ise) {
			// expected since it was disabled
		}

		
	}

	/**
	 * SimpleWriter writes a number of log files with a set of messages in each
	 */
	static class SimpleWriter implements Runnable {
		final ByteBuffer data = randomData(2000);

		int sequence = 0;

		private final HALogManager manager;
		private IRootBlockView rbv;
		private IHALogWriter writer;
		private ChecksumUtility checker;
		private int count;

		SimpleWriter(IRootBlockView rbv, HALogManager manager,
				ChecksumUtility checker, int count) throws IOException {
			this.manager = manager;
			this.rbv = rbv;
			this.writer = manager.createLog(rbv).getWriter();
			this.checker = checker;
			this.count = count;
		}

		@Override
		public void run() {
            final UUID storeUUID = UUID.randomUUID();
			try {
				for (int i = 0; i < count; i++) {
					// add delay to write thread to test read thread waiting for
					// data
					Thread.sleep(1);
					data.limit(200 + r.nextInt(1800));
					final IHAWriteMessage msg = new HAWriteMessage(storeUUID, rbv
							.getCommitCounter(), rbv.getLastCommitTime(),
							sequence++, data.limit(), checker.checksum(data),
							rbv.getStoreType(), rbv.getQuorumToken(), 1000, 0);

					writer.write(msg, data);
					if (((i + 1) % (1 + r.nextInt(count / 5))) == 0) {
						log.info("Cycling HALog after " + sequence
								+ " records");
						rbv = closeRBV(rbv);
						writer.close(rbv);
						sequence = 0;
						writer = manager.createLog(rbv).getWriter();
					}
				}
				rbv = closeRBV(rbv);
				writer.close(rbv);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	static class ReaderRun implements Runnable {

		final HALogManager manager;
		final AtomicInteger reads;
		final AtomicInteger openReaders;
		
		ReaderRun(final HALogManager manager, final AtomicInteger reads, final AtomicInteger openReaders) {
			this.manager = manager;
			this.reads = reads;
			this.openReaders = openReaders;
		}
		
		volatile ByteBuffer rbuf = ByteBuffer.allocate(100 * 1024); // 100K
		
		@Override
		public void run() {
			IHALogReader reader = null;
			try {
				final HALogFile file = manager.getOpenLogFile();
				if (file == null)
					return;
				
				reader = file.getReader();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			if (reader == null) {
				return;
			}

			try {
				openReaders.incrementAndGet();

				while (reader.hasMoreBuffers()) {
					rbuf.position(0);
					final IHAWriteMessage rmsg = reader
							.processNextBuffer(rbuf);
					reads.incrementAndGet();
					if (log.isDebugEnabled())
						log.debug("Read message: " + rmsg.getSequence()
								+ ", size: " + rmsg.getSize());
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				openReaders.decrementAndGet();
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * While a writer thread writes a number of HALogs, readers are opened to
	 * process them.
	 */
	@SuppressWarnings("deprecation")
	public void testStressConcurrentRWWriterReader() throws FileNotFoundException,
			IOException {
		// establish halogdir
		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final int recordWrites = 5000;

		Thread wthread = new Thread(
				new SimpleWriter(rbv, manager, checker, recordWrites));

		final AtomicInteger reads = new AtomicInteger(0);
		
		final AtomicInteger openReaders = new AtomicInteger(0);
		

		// start the writer first
		wthread.start();

		// now keep on opening readers for "current file" while writer continues
		while (wthread.isAlive()) {
			// Prevent too many readers starting up that can result in too much
			//	contention.
			if (openReaders.get() < 20) {
				Thread rthread = new Thread(new ReaderRun(manager, reads, openReaders));
				rthread.start();
			}
			
			try {
				// keep starting readers, there may be multiple readers over a single file
				Thread.sleep(r.nextInt(100));
			} catch (InterruptedException e) {
				break;
			}
		}
		
		while (openReaders.get() > 0)
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// fine
			}
		
		log.info("Writes: " + recordWrites + ", Reads: " + reads.get());

		assertTrue(reads.get() >= recordWrites);
		
	}

}
