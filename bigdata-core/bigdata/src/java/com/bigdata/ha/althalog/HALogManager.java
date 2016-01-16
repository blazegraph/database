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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

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

	private final File m_halogdir;
	
	private final Lock m_currentLock = new ReentrantLock();
	
	// protected by m_curretnLock
	private HALogFile m_current = null;
	
	/**
	 * Ensures private communication from created log files
	 */
	interface IHALogManagerCallback {
		File getHALogDir();
		void release(HALogFile logfile);
	}
	
	/*
	 * This callback is passed to HALogFiles when they are created.
	 * 
	 * A 
	 */
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
	
	/*
	 * Called by the logWriter via the callback when the
	 * closing rootblock is written
	 */
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
	 * @return the the open HALogFile
	 */
	public HALogFile getOpenLogFile() {
		m_currentLock.lock();
		try {
			return m_current;
		} finally {
			m_currentLock.unlock();
		}
	}
	
	/**
	 * Utility to retrieve a File reference to the current open file
	 * 
	 * @return
	 */
	public File getCurrentFile() {
		final HALogFile file = getOpenLogFile();
		
		return file == null ? null : file.getFile();
	}

	/**
	 * 
	 * @return the directory used to store the log files
	 */
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
		
		final File file = HALogFile.getHALogFileName(m_halogdir, commitCounter);
		final HALogFile halog = new HALogFile(file);
		
		return halog.getReader();
	}
	
	/**
	 * Returns the HALogFile for the commitCounter if it exists.
	 * It will return either the current or an historical file
	 * 
	 * @param commitCounter
	 * @return the HALogFile for this commit counter
	 * @throws FileNotFoundException
	 */
	public HALogFile getHALogFile(final long commitCounter) throws FileNotFoundException {
		/*
		 * Check the file exists first
		 */
		final File file = HALogFile.getHALogFileName(m_halogdir, commitCounter);
		if (!file.exists())
			throw new FileNotFoundException();
		
		m_currentLock.lock();
		try {
			if (m_current != null && m_current.getCommitCounter() == commitCounter)
				return m_current;
		} finally {
			m_currentLock.unlock();
		}
		
		/*
		 * If the file existed before we checked for current open file, then it must now
		 * be a read only log
		 */		
		return new HALogFile(file);
	}


	/**
	 * Closes and removes the current writer
	 * @throws IOException 
	 */
	public void disable() throws IOException {
		m_currentLock.lock();
		try {
			if (m_current != null)
				m_current.disable();
			
			m_current = null;
		} finally {
			m_currentLock.unlock();
		}
	}

	/*
	 * This logic is handled by the HAJournalServer, which is also aware
	 * of the backup integration through zookeeper.
	 */
//	/**
//	 * Disables any current log file, then removes all log files
//	 * from the directory
//	 * 
//	 * @throws IOException
//	 */
//	public void removeAllLogFiles() {
//		m_currentLock.lock();
//		try {
//			// No longer disables the current log file
//			
//			removeAllLogFiles(m_halogdir, getCurrentFile());
//		} finally {
//			m_currentLock.unlock();
//		}
//	}
//
//	/**
//	 * Recursively removes all log files from the provided directory
//	 * @param dir
//	 */
//	private void removeAllLogFiles(final File dir, final File preserve) {
//		final File[] files = logFiles(dir);
//		for (File f : files) {
//			try {
//				if (f.isDirectory()) {
//					removeAllLogFiles(f, preserve);
//					
//					// FIXME: should we remove the directory?
//					// Probably not
//					// f.delete();
//				} else if (f != preserve) {
//					f.delete();
//				}
//			} catch (final SecurityException se) {
//				haLog.warn("Unabel to delete file " + f.getAbsolutePath(), se);
//			}
//		}
//	}
	
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
	
	private static File[] logFiles(final File dir) {
		return dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {

				if (new File(dir, name).isDirectory()) {

					// Allow recursion through directories.
					return true;

				}

				return name.endsWith(IHALogReader.HA_LOG_EXT);

			}
		});
	}

	private static void doDirectory(final File dir, final IBufferAccess buf)
			throws IOException {

		final File[] files = logFiles(dir);

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
		final IHALogReader r = f.getReader();

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
