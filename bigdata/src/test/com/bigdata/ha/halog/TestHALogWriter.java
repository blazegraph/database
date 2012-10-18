package com.bigdata.ha.halog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import com.bigdata.ha.HALogReader;
import com.bigdata.ha.HALogWriter;
import com.bigdata.ha.IHALogReader;
import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.ChecksumUtility;

import junit.framework.TestCase;

public class TestHALogWriter extends TestCase {

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
		// establish halogdir
		File logdir = new File("/tmp/halogdir");
		logdir.mkdirs();

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogWriter writer = new HALogWriter(logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		writer.createLog(rbv);

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

		IHAWriteMessage msg = new HAWriteMessage(rbv.getCommitCounter(), rbv
				.getFirstCommitTime(), sequence, data.limit(), checker
				.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
				1000, 0);

		writer.write(msg, data);

		writer.closeLog(closeRBV(rbv));

		// for sanity, let's run through the standard reader
		try {
			HALogReader.main(new String[] { "/tmp/halogdir" });
		} catch (InterruptedException e) {
			// NOP
		}
	}

	/**
	 * Simple WriteReader, no concurrency, confirms non-delayed responses.
	 */
	public void testSimpleRWWriterReader() throws FileNotFoundException,
			IOException {
		// establish halogdir
		File logdir = new File("/tmp/halogdir");
		logdir.mkdirs();

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogWriter writer = new HALogWriter(logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		writer.createLog(rbv);

		int sequence = 0;

		final ByteBuffer data = randomData(2000);

		IHAWriteMessage msg = new HAWriteMessage(rbv.getCommitCounter(), rbv
				.getFirstCommitTime(), sequence, data.limit(), checker
				.checksum(data), rbv.getStoreType(), rbv.getQuorumToken(),
				1000, 0);

		writer.write(msg, data);

		final IHALogReader reader = writer.getReader();

		assertTrue(reader.hasMoreBuffers());

		ByteBuffer rbuf = ByteBuffer.allocate(1 * 1024 * 1024); // 1 mb
		IHAWriteMessage rmsg = reader.processNextBuffer(rbuf);

		assertTrue(rmsg.getSize() == msg.getSize());

		// commit the log file
		writer.closeLog(closeRBV(rbv));

		// the writer should have closed the file, so the reader should return
		// immediately to report no more buffers
		assertFalse(reader.hasMoreBuffers());

		// for sanity, let's run through the standard reader
		try {
			HALogReader.main(new String[] { "/tmp/halogdir" });
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

		private IRootBlockView rbv;
		private HALogWriter writer;
		private ChecksumUtility checker;
		private int count;

		SimpleWriter(IRootBlockView rbv, HALogWriter writer, ChecksumUtility checker, int count) {
			this.rbv = rbv;
			this.writer = writer;
			this.checker = checker;
			this.count = count;
		}
		
		@Override
		public void run() {
			try {
				for (int i = 0; i < count; i++) {
					// add delay to write thread to test read thread waiting for data
					Thread.sleep(10);
					IHAWriteMessage msg = new HAWriteMessage(rbv
							.getCommitCounter(), rbv.getLastCommitTime(),
							sequence++, data.limit(), checker
									.checksum(data), rbv.getStoreType(),
							rbv.getQuorumToken(), 1000, 0);

					writer.write(msg, data);
					if (((i+1) % (1 + r.nextInt(count/3))) == 0) {
						System.out.println("Cycling HALog after " + sequence + " records");
						rbv = closeRBV(rbv);
						writer.closeLog(rbv);
						sequence = 0;
						writer.createLog(rbv);
					}
				}
				rbv = closeRBV(rbv);
				writer.closeLog(rbv);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	/**
	 * While a writer thread writes a number of HALogs, readers are opened
	 * to process them.
	 */
	public void testConcurrentRWWriterReader() throws FileNotFoundException,
			IOException {
		// establish halogdir
		File logdir = new File("/tmp/halogdir");
		logdir.mkdirs();

		final ChecksumUtility checker = ChecksumUtility.getCHK();

		final HALogWriter writer = new HALogWriter(logdir);
		final IRootBlockView rbv = openRBV(StoreTypeEnum.RW);

		assertTrue(rbv.getStoreType() == StoreTypeEnum.RW);

		writer.createLog(rbv);

		final ByteBuffer data = randomData(2000);

		Thread wthread = new Thread(new SimpleWriter(rbv, writer, checker, 500));

		Runnable rreader = new Runnable() {

			ByteBuffer rbuf = ByteBuffer.allocate(1 * 1024 * 1024); // 1 mb

			@Override
			public void run() {
				final IHALogReader reader = writer.getReader();
				if (reader == null) {
					return;
				}
				
				try {
					while (reader.hasMoreBuffers()) {
						IHAWriteMessage rmsg = reader.processNextBuffer(rbuf);

//						System.out.println("Read message: " + rmsg.getSequence()
//								+ ", size: " + rmsg.getSize());
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		};
		
		// start the writer first
		wthread.start();
		
		// now keep on opening readers for "current file" while writer continues
		while (wthread.isAlive()) {
			Thread rthread = new Thread(rreader);
			rthread.start();
			while (rthread.isAlive()) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		// for sanity, let's run through the standard reader
		try {
			HALogReader.main(new String[] { "/tmp/halogdir" });
		} catch (InterruptedException e) {
			// NOP
		}
	}

}
