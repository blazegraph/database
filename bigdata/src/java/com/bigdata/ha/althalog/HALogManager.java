package com.bigdata.ha.althalog;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import sun.util.logging.resources.logging;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.StoreTypeEnum;

/**
 * Provides the top level control
 * 
 * @author Martyn Cutcher
 *
 */
public class HALogManager {
	/**
	 * Logger for HA events.
	 */
	private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

	private File m_halogdir;
	
	private HALogFile m_current = null;
	
	private Lock m_currentLock = new ReentrantLock();
	
	/**
	 * Ensures private communication from created log files
	 */
	interface IHALogManagerCallback {
		File getHALogDir();
		void release(HALogFile logfile);
	}
	
	private IHALogManagerCallback m_callback = new IHALogManagerCallback() {

		@Override
		public void release(final HALogFile logfile) {
			HALogManager.this.closeLog(logfile);
		}

		@Override
		public File getHALogDir() {
			return HALogManager.this.getHALogDir();
		}
		
	};
	
	public HALogManager(final File halogdir) {
		m_halogdir = halogdir;
		
		if (!m_halogdir.exists())
			throw new IllegalArgumentException();
		
		if (haLog.isInfoEnabled())
			haLog.info("HALogManager initialized");
	}
	
	public HALogFile createLog(final IRootBlockView rbv) throws IOException {
		
		if (haLog.isInfoEnabled())
			haLog.info("Creating log for commit " + rbv.getCommitCounter());

		m_currentLock.lock();
		try {
			if (m_current != null)
				throw new IllegalStateException();
			
			final HALogFile ret = new HALogFile(rbv, m_callback);
			
			m_current = ret;
			
			return ret;
		} finally {
			m_currentLock.unlock();
		}
	}
	
	private void closeLog(final HALogFile current) {

		m_currentLock.lock();
		try {
			if (m_current != current)
				throw new IllegalStateException();
			
			m_current = null;
		} finally {
			m_currentLock.unlock();
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public HALogFile getOpenLogFile() {
		m_currentLock.lock();
		try {
			return m_current;
		} finally {
			m_currentLock.unlock();
		}
	}

	public File getHALogDir() {
		return m_halogdir;
	}
	

	
	public IHALogReader getReader(final long commitCounter) throws IOException {
		m_currentLock.lock();
		try {
			if (m_current != null && m_current.getCommitCounter() == commitCounter)
				return m_current.getReader();
		} finally {
			m_currentLock.unlock();
		}
		
		final File file = new File(m_halogdir, HALogFile.getHALogFileName(commitCounter));
		final HALogFile halog = new HALogFile(file);
		
		return halog.getReader();
	}


	/**
	 * Closes the current writer
	 * @throws IOException 
	 */
	public void disable() throws IOException {
		m_currentLock.lock();
		try {
			if (m_current != null)
				m_current.disable();
			
			assert m_current == null;
		} finally {
			m_currentLock.unlock();
		}
	}

	/**
	 * Utility program will dump log files (or directories containing log files)
	 * provided as arguments.
	 * 
	 * @param args
	 *            Zero or more files or directories.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws IOException,
			InterruptedException {

		final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

		try {

			for (String arg : args) {

				final File file = new File(arg);

				if (!file.exists()) {

					System.err.println("No such file: " + file);

					continue;

				}

				if (file.isDirectory()) {

					doDirectory(file, buf);

				} else {

					doFile(file, buf);

				}

			}

		} finally {

			buf.release();

		}

	}

	private static void doDirectory(final File dir, final IBufferAccess buf)
			throws IOException {

		final File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {

				if (new File(dir, name).isDirectory()) {

					// Allow recursion through directories.
					return true;

				}

				return name.endsWith(HALogWriter.HA_LOG_EXT);

			}
		});

		for (File file : files) {

			if (file.isDirectory()) {

				doDirectory(file, buf);

			} else {

				doFile(file, buf);

			}

		}

	}

	private static void doFile(final File file, final IBufferAccess buf)
			throws IOException {

		final HALogFile f = new HALogFile(file);
		final HALogReader r = f.getReader();

		try {

			final IRootBlockView openingRootBlock = r.getOpeningRootBlock();

			final IRootBlockView closingRootBlock = r.getClosingRootBlock();

			final boolean isWORM = openingRootBlock.getStoreType() == StoreTypeEnum.WORM;

			if (openingRootBlock.getCommitCounter() == closingRootBlock
					.getCommitCounter()) {

				System.err.println("EMPTY LOG: " + file);

			}

			System.out.println("----------begin----------");
			System.out.println("file=" + file);
			System.out.println("openingRootBlock=" + openingRootBlock);
			System.out.println("closingRootBlock=" + closingRootBlock);
			

			while (r.hasMoreBuffers()) {

				// don't pass buffer in if WORM, just validate the messages
				final IHAWriteMessage msg = r.processNextBuffer(isWORM ? null
						: buf.buffer());

				System.out.println(msg.toString());

			}
			System.out.println("-----------end-----------");

		} finally {

			r.close();

		}

	}

}
