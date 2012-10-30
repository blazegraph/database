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
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.ChecksumUtility;

import junit.framework.TestCase;

public class TestAltHALogWriter extends TestCase {

	private static final Logger log = Logger
			.getLogger(TestAltHALogWriter.class);

	final File m_logdir = new File("/tmp/halogdir");
	// final File m_logdir = new File("/Volumes/SSDData/tmp/halogdir");

	protected void setUp() {
		m_logdir.mkdirs();
	}

	protected void tearDown() {
		// clear directory
		final File[] files = m_logdir.listFiles();
		for (int i = 0; i < files.length; i++) {
			files[i].delete();
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
	public void testSimpleRWWriter() throws FileNotFoundException, IOException {

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final HALogWriter writer = manager.createLog(rbv).getWriter();

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

		IHAWriteMessage msg = new HAWriteMessage(rbv.getCommitCounter(), rbv
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
	public void testSimpleRWWriterReader() throws FileNotFoundException,
			IOException {

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final HALogFile logfile = manager.createLog(rbv);
		final HALogWriter writer = logfile.getWriter();

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

		IHAWriteMessage msg = new HAWriteMessage(rbv.getCommitCounter(), rbv
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

		// for sanity, let's run through the standard reader
		try {
			HALogManager.main(new String[] { m_logdir.getAbsolutePath() });
		} catch (InterruptedException e) {
			// NOP
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
		private HALogWriter writer;
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
			try {
				for (int i = 0; i < count; i++) {
					// add delay to write thread to test read thread waiting for
					// data
					Thread.sleep(1);
					data.limit(200 + r.nextInt(1800));
					IHAWriteMessage msg = new HAWriteMessage(rbv
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

	class ReaderRun implements Runnable {

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
			}
		}

	}

	/**
	 * While a writer thread writes a number of HALogs, readers are opened to
	 * process them.
	 */
	public void testStressConcurrentRWWriterReader() throws FileNotFoundException,
			IOException {
		// establish halogdir
		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogManager manager = new HALogManager(m_logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		final int recordWrites = 10000;

		Thread wthread = new Thread(
				new SimpleWriter(rbv, manager, checker, recordWrites));

		final AtomicInteger reads = new AtomicInteger(0);
		
		final AtomicInteger openReaders = new AtomicInteger(0);
		

		// start the writer first
		wthread.start();

		// now keep on opening readers for "current file" while writer continues
		while (wthread.isAlive()) {
			Thread rthread = new Thread(new ReaderRun(manager, reads, openReaders));
			rthread.start();
			
			try {
				// keep starting readers, there may be multiple readers over a single file
				Thread.sleep(r.nextInt(100));
			} catch (InterruptedException e) {
				break;
			}

			// Previously we just cycled around with new readers, but only one at a time
//			while (rthread.isAlive()) {
//				try {
//					Thread.sleep(10);
//				} catch (InterruptedException e) {
//					break;
//				}
//			}
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
