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
package com.bigdata.ha.halog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.rawstore.Bytes;

/**
 * Wrapper class to handle process log creation and output for HA.
 * 
 * The process log stores the HAWriteMessages and buffers to support reading and
 * reprocessing as part of the HA synchronization protocol.
 * 
 * The writer encapsulates not only the writing of individual messages but also
 * the closing and creation of new files.
 * 
 * @author Martyn Cutcher
 */
public class HALogWriter implements IHALogWriter {

	/**
	 * Logger for HA events.
	 */
	private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

	/*
	 * Note: All of this stuff is to be more or less compatible with the magic
	 * and version at the start of a Journal file. We use a different MAGIC
	 * value for the HA Log, but the same offset to the first and second root
	 * blocks. The data starts after the 2nd root block.
	 */

	static final int SIZE_MAGIC = Bytes.SIZEOF_INT;
	static final int SIZE_VERSION = Bytes.SIZEOF_INT;
	static final int SIZEOF_ROOT_BLOCK = RootBlockView.SIZEOF_ROOT_BLOCK;

	/**
	 * Offset of the first root block in the file.
	 */
	static final int OFFSET_ROOT_BLOCK0 = SIZE_MAGIC + SIZE_VERSION;

	/**
	 * Offset of the second root block in the file.
	 */
	static final int OFFSET_ROOT_BLOCK1 = SIZE_MAGIC + SIZE_VERSION
			+ (SIZEOF_ROOT_BLOCK * 1);

	/**
	 * The size of the file header, including MAGIC, version, and both root
	 * blocks. The data starts at this offset.
	 */
	static final int headerSize0 = SIZE_MAGIC + SIZE_VERSION
			+ (SIZEOF_ROOT_BLOCK * 2);

	/**
	 * Magic value for HA Log (the root blocks have their own magic value).
	 */
	static public final int MAGIC = 0x83d9b735;

	/**
	 * HA log version number (version 1).
	 */
	static public final int VERSION1 = 0x1;

	/** HA log directory. */
	private final File m_haLogDir;

	/**
	 * The root block of the leader at the start of the current write set.
	 */
	private IRootBlockView m_rootBlock;

	/** Current write cache block sequence counter. */
	private long m_nextSequence = 0;

	/** current log file. */
	private FileState m_state = null;

	/** state lock **/
	final private ReentrantReadWriteLock m_stateLock = new ReentrantReadWriteLock();

//	public static final String HA_LOG_EXT = ".ha-log";

	/** current write point on the channel. */
	private long m_position = headerSize0;

	/**
	 * Return the commit counter that is expected for the writes that will be
	 * logged (the same commit counter that is on the opening root block).
	 */
	public long getCommitCounter() {

		assertOpen();

		return m_rootBlock.getCommitCounter();

	}

	/**
	 * Return the sequence number that is expected for the next write.
	 */
	public long getSequence() {

		assertOpen();

		return m_nextSequence;

	}

    /**
     * @throws IllegalStateException
     *             if the HALog is not open.
     */
	private void assertOpen() {

		if (m_state == null)
			throw new IllegalStateException();

	}
	
	public boolean isHALogOpen() {
	  
	    return m_state != null && !m_state.isCommitted();
	    
	}

	/**
	 * Return the log file (if any).
	 */
	public File getFile() {
		final Lock lock = m_stateLock.readLock();
		lock.lock();
		try {
			return m_state == null ? null : m_state.m_haLogFile;
		} finally {
			lock.unlock();
		}

	}

    /**
     * Return the HALog file associated with the commit counter.
     * 
     * @param dir
     *            The HALog directory.
     * @param commitCounter
     *            The closing commit counter (the HALog file is named for the
     *            commit counter that will be associated with the closing root
     *            block).
     * 
     * @return The HALog {@link File}.
     */
    public static File getHALogFileName(final File dir,
            final long commitCounter) {

        return CommitCounterUtility.getCommitCounterFile(dir, commitCounter,
                IHALogReader.HA_LOG_EXT);
//		/*
//		 * Format the name of the log file.
//		 * 
//		 * Note: The commit counter in the file name should be zero filled to 20
//		 * digits so we have the files in lexical order in the file system (for
//		 * convenience).
//		 */
//		final String logFile;
//		{
//
//			final StringBuilder sb = new StringBuilder();
//
//			final Formatter f = new Formatter(sb);
//
//			f.format("%020d" + IHALogReader.HA_LOG_EXT, commitCounter);
//			f.flush();
//			f.close();
//
//			logFile = sb.toString();
//
//		}
//
//		return logFile;

	}

	public String toString() {

		final IRootBlockView tmp = m_rootBlock;

		final long seq = m_nextSequence;

		return getClass().getName() + "{" + m_state == null ? "closed"
				: "commitCounter=" + tmp.getCommitCounter() + ",nextSequence="
						+ seq + "}";

	}

	public HALogWriter(final File logDir) {

		m_haLogDir = logDir;

	}

	/**
	 * Open an HA log file for the write set starting with the given root block.
	 * 
	 * @param rootBlock
	 *            The root block.
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void createLog(final IRootBlockView rootBlock)
			throws FileNotFoundException, IOException {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		if (m_rootBlock != null) // may not be open.
			throw new IllegalStateException();

		if (haLog.isInfoEnabled())
			haLog.info("rootBlock=" + rootBlock);

		m_rootBlock = rootBlock;

		m_nextSequence = 0L;

		/*
		 * Format the name of the log file.
		 * 
		 * Note: The commit counter in the file name should be zero filled to 20
		 * digits so we have the files in lexical order in the file system (for
		 * convenience).
		 * 
		 * Note: We use commitCounter+1 so the file will be labeled by the
		 * commit point that will be achieved when that log file is applied to a
		 * journal whose current commit point is [commitCounter].
		 */

		final long commitCounter = rootBlock.getCommitCounter();

//        final String logFile = getHALogFileName(commitCounter + 1);
//
//        final File log = new File(m_haLogDir, logFile);
        final File log = getHALogFileName(m_haLogDir, commitCounter + 1);

//        final File log = new File(m_haLogDir, logFile);

		// Must delete file if it exists.
		if (log.exists() && !log.delete()) {

			/*
			 * It is a problem if a file exists and we can not delete it. We
			 * need to be able to remove the file and replace it with a new file
			 * when we log the write set for this commit point.
			 */

			throw new IOException("Could not delete: " + log);

		}

		final File parentDir = log.getParentFile();
		
        // Make sure the parent directory(ies) exist.
        if (!parentDir.exists())
            if (!parentDir.mkdirs())
                throw new IOException("Could not create directory: "
                        + parentDir);

		final Lock lock = m_stateLock.writeLock();
		lock.lock();
		try {
			m_state = new FileState(log, rootBlock.getStoreType());

			/*
			 * Write the MAGIC and version on the file.
			 */
			m_state.m_raf.seek(0);
			m_state.m_raf.writeInt(MAGIC);
			m_state.m_raf.writeInt(VERSION1);

			/*
			 * Write root block to slots 0 and 1.
			 * 
			 * Initially, the same root block is in both slots. This is a valid,
			 * but logically empty, HA Log file.
			 * 
			 * When the HA Log file is properly sealed with a root block, that
			 * root block is written onto slot 1.
			 */

			writeRootBlock(true/* isRootBlock0 */, rootBlock);

			writeRootBlock(false/* isRootBlock0 */, rootBlock);
		} finally {
			lock.unlock();
		}

	}

	/**
	 * Hook for
	 * {@link FileChannelUtility#writeAll(IReopenChannel, ByteBuffer, long)}
	 */
	private final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

		@Override
		public FileChannel reopenChannel() throws IOException {

		    final Lock lock = m_stateLock.readLock();
			
		    lock.lock();
			
		    try {

                if (m_state == null || m_state.m_channel == null
                        || !m_state.m_channel.isOpen()) {

                    throw new IOException("Closed");

                }
   
				return m_state.m_channel;
				
			} finally {
				
			    lock.unlock();
			    
			}

		}
	};

    /**
     * Write the final root block on the HA log and close the file. This "seals"
     * the file, which now represents the entire write set associated with the
     * commit point in the given root block.
     * 
     * @param rootBlock
     *            The final root block for the write set.
     * @throws IOException
     */
	public void closeHALog(final IRootBlockView rootBlock)
			throws FileNotFoundException, IOException {

		final Lock lock = m_stateLock.writeLock();
		lock.lock();
		try {
			if (rootBlock == null)
				throw new IllegalArgumentException();

			if (m_rootBlock == null) // no root block associated with log.
				throw new IllegalStateException();

			if (haLog.isInfoEnabled())
				haLog.info("rootBlock=" + rootBlock);

			final long expectedCommitCounter = this.m_rootBlock
					.getCommitCounter() + 1;

			if (expectedCommitCounter != rootBlock.getCommitCounter()) {

				throw new IllegalStateException("CommitCounter: expected="
						+ expectedCommitCounter + ", actual="
						+ rootBlock.getCommitCounter());

			}

			// if (rootBlock.getLastCommitTime() != this.m_rootBlock
			// .getLastCommitTime()) {
			//
			// throw new IllegalStateException();
			//
			// }

			if (!this.m_rootBlock.getUUID().equals(rootBlock.getUUID())) {

				throw new IllegalStateException("Store UUID: expected="
						+ (m_rootBlock.getUUID()) + ", actual="
						+ rootBlock.getUUID());

			}

			flush(); // current streamed data

			/*
			 * The closing root block is written into which ever slot
			 * corresponds to its whether that root block is root block zero.
			 * Both root blocks are identical up to this point, so we can write
			 * the closing root block into either slot. HALogReader will use the
			 * commit counters to figure out which root block is the opening
			 * root block and which root block is the closing root block.
			 */
			writeRootBlock(rootBlock.isRootBlock0(), rootBlock);

			// // The closing root block is always in slot 1.
			// writeRootBlock(false/* isRootBlock0 */, rootBlock);

			m_state.committed();

			close();
		} finally {
			lock.unlock();
		}

	}

	/**
	 * Writes the root block at the given offset.
	 */
	private void writeRootBlock(final boolean isRootBlock0,
			final IRootBlockView rootBlock) throws IOException {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		final long position = isRootBlock0 ? OFFSET_ROOT_BLOCK0
				: OFFSET_ROOT_BLOCK1;

		FileChannelUtility.writeAll(reopener, rootBlock.asReadOnlyBuffer(),
				position);

		if (haLog.isDebugEnabled())
			haLog.debug("wrote root block: " + rootBlock);

	}

	/**
     * Write the message and the data on the live HALog.
     * 
     * @param msg
     *            The message.
     * @param data
     *            The data.
     * @throws IllegalStateException
     *             if the message is not appropriate for the state of the log.
     * @throws IOException
     *             if we can not write on the log.
	 */
	public void writeOnHALog(final IHAWriteMessage msg, final ByteBuffer data)
			throws IOException, IllegalStateException {

		final Lock lock = m_stateLock.readLock();
		lock.lock();
		try {
			assertOpen();

            /*
             * Check if this really is a valid message for this file. If it is
             * not, then throw out an exception.
             */
			if (m_rootBlock.getCommitCounter() != msg.getCommitCounter())
				throw new IllegalStateException("commitCounter="
						+ m_rootBlock.getCommitCounter() + ", but msg=" + msg);

			if (m_rootBlock.getLastCommitTime() != msg.getLastCommitTime())
				throw new IllegalStateException("lastCommitTime="
						+ m_rootBlock.getLastCommitTime() + ", but msg=" + msg);

			if (m_nextSequence != msg.getSequence())
				throw new IllegalStateException("nextSequence="
						+ m_nextSequence + ", but msg=" + msg);

			if (haLog.isInfoEnabled())
				haLog.info("msg=" + msg + ", position=" + m_position);

			if (m_position < headerSize0)
				throw new AssertionError("position=" + m_position
						+ ", but headerSize=" + headerSize0);

			/*
			 * Write the HAWriteMessage onto the channel.
			 */
			{
				// serialized message object (pos=0; limit=nbytes)
				final ByteBuffer tmp = bufferObject(msg);

				final int nbytes = tmp.limit();

				FileChannelUtility.writeAll(reopener, tmp, m_position);

				m_position += nbytes;

				m_nextSequence++;

			}

			switch (m_rootBlock.getStoreType()) {
			case RW: {
				/*
				 * Write the WriteCache block on the channel.
				 */
				final int nbytes = msg.getSize();
				assert data.position() == 0;
				assert data.limit() == nbytes;
				// Note: duplicate() to avoid side effects on ByteBuffer!!!
				FileChannelUtility.writeAll(reopener, data.duplicate(),
						m_position);
				m_position += nbytes;
			}
			case WORM: {
				/*
				 * We will use the HA failover read API to recover the block
				 * from a node in the quorum when we need to replay the HA log.
				 */
				break;
			}
			default:
				throw new AssertionError();
			}

			// let any readers know a new record is ready
			m_state.addRecord();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Utility to return a ByteBuffer containing the external version of the
	 * object.
	 * 
	 * @return The {@link ByteBuffer}. The position will be zero. The limit will
	 *         be the #of bytes in the serialized object.
	 */
	private ByteBuffer bufferObject(final Object obj) throws IOException {

		// Note: pos=0; limit=capacity=length.
		return ByteBuffer.wrap(SerializerUtil.serialize(obj));

	}

	/**
	 * Close the file (does not flush).
	 */
	private void close() throws IOException { // Note: caller owns m_stateLock!
		try {
			if (m_state != null) {
				m_state.close();
			}
		} finally {
			reset(); // Note: reset() clears [m_state]!
		}
	}

	/**
	 * Clear internal fields.
	 */
	private void reset() {
		
		assert m_stateLock.isWriteLocked();

		m_state = null;

		m_position = headerSize0;

		m_rootBlock = null;

		m_nextSequence = 0L;

	}

	/**
	 * When the HA leader commits it must flush the log
	 */
	private void flush() throws IOException {

		if (m_state != null) {

			m_state.m_channel.force(true);

		}

	}

	/**
	 * On various error conditions we may need to remove the log
	 * 
	 * @throws IOException
	 */
	private void remove() throws IOException {

		final Lock lock = m_stateLock.writeLock();
		lock.lock();
		try {

			if (m_state != null) {

				/*
				 * Conditional remove iff file is open. Will not remove
				 * something that has been closed.
				 */

				m_state.m_channel.close();

				if (m_state.m_haLogFile.exists() && !m_state.m_haLogFile.delete()) {

					/*
					 * It is a problem if a file exists and we can not delete
					 * it. We need to be able to remove the file and replace it
					 * with a new file when we log the write set for this commit
					 * point.
					 */

					throw new IOException("Could not delete: " + m_state.m_haLogFile);

				}

			}

		} finally {

			reset();

			lock.unlock();
		}

	}

	/**
	 * Disable (and remove) the current log file if one is open.
	 */
	public void disableHALog() throws IOException {

		if (haLog.isInfoEnabled())
			haLog.info("");

		/*
		 * Remove unless log file was already closed.
		 */

		remove();

	}

    /**
     * Return the {@link IHALogReader} for the specified commit counter. If the
     * request identifies the HALog that is currently being written, then an
     * {@link IHALogReader} will be returned that will "see" newly written
     * entries on the HALog. If the request identifies a historical HALog that
     * has been closed and which exists, then a reader will be returned for that
     * HALog file. Otherwise, an exception is thrown.
     * 
     * @param commitCounter
     *            The commit counter associated with the commit point at the
     *            close of the write set (the commit counter that is in the file
     *            name).
     * 
     * @return The {@link IHALogReader}.
     * 
     * @throws IOException
     *             if the commitCounter identifies an HALog file that does not
     *             exist or can not be read.
     */
    public IHALogReader getReader(final long commitCounter)
            throws FileNotFoundException, IOException {

        final File logFile = //new File(m_haLogDir,
                HALogWriter.getHALogFileName(m_haLogDir, commitCounter);

        final Lock lock = m_stateLock.readLock();
        lock.lock();
        try {
            
            if (!logFile.exists()) {

                // No log for that commit point.
                throw new FileNotFoundException(logFile.getName());

            }

            if (m_state != null
                    && m_rootBlock.getCommitCounter() + 1 == commitCounter) {

                /*
                 * This is the live HALog file.
                 */

                if (haLog.isDebugEnabled())
                    haLog.debug("Opening live HALog: file="
                            + m_state.m_haLogFile);
                
                return new OpenHALogReader(m_state);
                
            }

            if (haLog.isDebugEnabled())
                haLog.debug("Opening historical HALog: file=" + logFile);

            return new HALogReader(logFile);
            
        } finally {

            lock.unlock();
            
        }

    }
	
	/**
	 * The FileState class encapsulates the file objects shared by the Writer
	 * and Readers.
	 */
	private static class FileState {
        private final StoreTypeEnum m_storeType;
        private final File m_haLogFile;
        private final FileChannel m_channel;
        private final RandomAccessFile m_raf;
        /*
         * Note: Mutable fields are guarded by synchronized(this) for the
         * FileState object.
         */
        /**
         * The #of messages written onto the live HALog file.
         */
        private long m_records = 0;
        /**
         * <code>false</code> until the live HALog file has been committed (by
         * writing the closing root block).
         */
		private boolean m_committed = false;
        /** number of open writers (at most one) plus readers **/
        private int m_accessors;

        private final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

			@Override
			public FileChannel reopenChannel() throws IOException {

				if (m_channel == null)
					throw new IOException("Closed");

				return m_channel;

			}
		};

        private FileState(final File file, final StoreTypeEnum storeType)
                throws FileNotFoundException {
            
			m_haLogFile = file;
			m_storeType = storeType;
			m_raf = new RandomAccessFile(m_haLogFile, "rw");
			m_channel = m_raf.getChannel();
			m_accessors = 1; // the writer is a reader also
			
		}

		public void close() throws IOException {
            synchronized (this) {
                try {
                    if (m_accessors == 0) {
                        /*
                         * Already at zero. Do not decrement further.
                         */
                        throw new IllegalStateException();
                    }
                    // One less reader/writer.
                    --m_accessors;
                    if (m_accessors == 0) {
                        if (haLog.isDebugEnabled())
                            haLog.debug("Closing file");
                        /*
                         * Note: Close the RandomAccessFile rather than the
                         * FileChannel. Potential fix for leaking open file
                         * handles.
                         */
                        // m_channel.close();
                        m_raf.close();
                    }
                } finally {
                    // wake up anyone waiting.
                    this.notifyAll();
                }
            }
		}

		public void addRecord() {
			synchronized (this) {
				m_records++;
				this.notifyAll();
			}
		}

		public long recordCount() {
			synchronized (this) {
				return m_records;
			}
		}

		public void committed() {
			synchronized (this) {
				m_committed = true;
				this.notifyAll();
			}
		}

		public boolean isCommitted() {
			synchronized (this) {
				return m_committed;
			}
		}

		public boolean isEmpty() {
			synchronized(this) {
				return m_committed && m_records == 0;
			}
		}

        /**
         * 
         * @param record
         *            - the next sequence required
         */
        /*
         * TODO We should support wait up to a timeout here to make the API more
         * pleasant.
         */
		public void waitOnStateChange(final long record) {
			
		    synchronized (this) {
			
		        if (m_records >= record) {
				
		            return;
		            
				}

				try {

				    wait();
				    
				} catch (InterruptedException e) {
				    
				    // Propagate the interrupt.
				    Thread.currentThread().interrupt();
				    
				    return;
				    
				}

			}

		}

	} // class FileState

	static class OpenHALogReader implements IHALogReader {

	    private final FileState m_state;
	    
	    private long m_record = 0L;
	    
	    private long m_position = headerSize0; // initial position
        
	    /** <code>true</code> iff this reader is open. */
        private final AtomicBoolean open = new AtomicBoolean(true);

        OpenHALogReader(final FileState state) {

            if (state == null)
                throw new IllegalArgumentException();
			
            m_state = state;
            
            // Note: Must be synchronized for visibility and atomicity!
            synchronized (m_state) {

                m_state.m_accessors++;
                
            }
			
		}

		@Override
		public boolean isLive() {

		    return true;
		    
		}
		
        @Override
        public IRootBlockView getOpeningRootBlock() throws IOException {

            final RootBlockUtility tmp = new RootBlockUtility(m_state.reopener,
                    m_state.m_haLogFile, true/* validateChecksum */,
                    false/* alternateRootBlock */, false/* ignoreBadRootBlock */);

            final IRootBlockView closeRootBlock = tmp.chooseRootBlock();

            final IRootBlockView openRootBlock = tmp.rootBlock0 == closeRootBlock ? tmp.rootBlock1
                    : tmp.rootBlock0;

            return openRootBlock;

        }

		@Override
		public IRootBlockView getClosingRootBlock() throws IOException {

		    final RootBlockUtility tmp = new RootBlockUtility(m_state.reopener,
					m_state.m_haLogFile, true/* validateChecksum */,
					false/* alternateRootBlock */, false/* ignoreBadRootBlock */);

			return tmp.chooseRootBlock();
		}

		@Override
		public boolean hasMoreBuffers() throws IOException {

            if (!isOpen())
                return false;
		    
		    if (m_state.isCommitted() && m_state.recordCount() <= m_record)
				return false;

			if (m_state.recordCount() > m_record)
				return true;

			m_state.waitOnStateChange(m_record + 1);

			return hasMoreBuffers(); // tail recursion.
			
		}

		@Override
		public boolean isOpen() {
		    
		    return open.get();
		    
		}
		
		@Override
		public boolean isEmpty() {

		    return m_state.isEmpty();
		    
		}

		@Override
		public IHAWriteMessage processNextBuffer(ByteBuffer clientBuffer)
				throws IOException {

			final IHAWriteMessage msg;

			synchronized (m_state) {

			    final long savePosition = m_state.m_channel.position();
				
			    m_state.m_channel.position(m_position);

				msg = HALogReader.processNextBuffer(m_state.m_raf,
						m_state.reopener, m_state.m_storeType, clientBuffer);

				m_position = m_state.m_channel.position();
				
				m_state.m_channel.position(savePosition);
				
			}

			m_record++;

			return msg;
		}

		@Override
        public void close() throws IOException {

		    // Note: this pattern prevents a double-close of a reader.
		    if (open.compareAndSet(true/* expected */, false/* newValue */)) {
            
		        /*
		         * Close an open reader.
		         */
                m_state.close();
                
            }
		    
		}

        @Override
        public void computeDigest(final MessageDigest digest)
                throws DigestException, IOException {

            HALogReader.computeDigest(m_state.reopener, digest);

        }

    } // class OpenHAReader

}
