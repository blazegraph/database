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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.althalog.HALogManager.IHALogManagerCallback;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.util.Bytes;
import com.bigdata.util.ChecksumError;

/**
 * 
 * @author Martyn Cutcher
 * 
 */
public class HALogFile {

//	public static final String HA_LOG_EXT = ".ha-log";

	/**
	 * Logger for HA events.
	 */
	private static final Logger log = Logger.getLogger("com.bigdata.haLog");

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

	public static final long START_DATA = headerSize0;

	/**
	 * Magic value for HA Log (the root blocks have their own magic value).
	 */
	static final int MAGIC = 0x83d9b735;

	/**
	 * HA log version number (version 1).
	 */
	static final int VERSION1 = 0x1;

	private final int m_magic;
	private final int m_version;

	private final IHALogManagerCallback m_callback;

	private final IRootBlockView m_openRootBlock;
	// The closing root block is updated by the writer when it closes
	private IRootBlockView m_closeRootBlock;

	private final StoreTypeEnum m_storeType;
	private final File m_haLogFile;
	private final FileChannel m_channel;
	private final RandomAccessFile m_raf;

	/*
	 * Tracks the number of open readers and writers
	 */
	private volatile int m_accessors = 0;

	final IReopenChannel<FileChannel> m_reopener = new IReopenChannel<FileChannel>() {

		@Override
		public FileChannel reopenChannel() throws IOException {

			if (m_channel == null)
				throw new IOException("Closed");

			return m_channel;

		}
	};

	// A writer is only created for a newly opened file
	private final HALogWriter m_writer;

	/**
	 * We need to take a lock whenever we are updating the file and signal the
	 * fileChange condition to alert any waiting readers.
	 */
	private final ReadWriteLock m_lock = new ReentrantReadWriteLock();
	private final Lock m_writeLock = m_lock.writeLock();
	private final Lock m_readLock = m_lock.readLock();

	// Condition to signal changes in file:
	// either new messages or closing rootblock
	private final Condition m_fileChange = m_writeLock.newCondition();

	// protected by m_writeLock
	private long m_writePosition = -1;
	private long m_sequence = 0;

	/**
	 * This constructor is called by the log manager to create the file. A
	 * writer is created at the same time, and its presence indicates that the
	 * file is open for writing.
	 * 
	 * @throws IOException
	 */
	public HALogFile(final IRootBlockView rbv,
			final IHALogManagerCallback callback) throws IOException {
		m_callback = callback;
        m_haLogFile = getHALogFileName(m_callback.getHALogDir(),
                rbv.getCommitCounter());

		if (m_haLogFile.exists())
			throw new IllegalStateException("File already exists: "
					+ m_haLogFile.getAbsolutePath());

        final File parentDir = m_haLogFile.getParentFile();

        // Make sure the parent directory(ies) exist.
        if (!parentDir.exists())
            if (!parentDir.mkdirs())
                throw new IOException("Could not create directory: "
                        + parentDir);

        m_raf = new RandomAccessFile(m_haLogFile, "rw");
		m_channel = m_raf.getChannel();
		m_storeType = rbv.getStoreType();

		m_openRootBlock = rbv;
		m_closeRootBlock = null; // file NOT closed

		m_magic = MAGIC;
		m_version = VERSION1;

		/*
		 * Write the MAGIC and version on the file.
		 */
		m_raf.seek(0);
		m_raf.writeInt(m_magic);
		m_raf.writeInt(m_version);

		// Write opening rootblock as both BLOCK0 and BLOCK1
		writeRootBlock(true, rbv); // as BLOCK0
		writeRootBlock(false, rbv); // as BLOCK1

		m_writePosition = START_DATA;

		m_writer = new HALogWriter();

		if (log.isInfoEnabled())
			log.info("Opening HALogFile: " + m_haLogFile.getAbsolutePath());

	}

	/**
	 * This constructor creates a read only view of the log file and can be
	 * used independently of the HALogManager.
	 * 
	 * The opening and closing root blocks are examined to confirm the file
	 * has fully committed writes and can be opened for read only access.
	 * 
	 * @param readonlyLog
	 * @throws FileNotFoundException
	 */
	public HALogFile(final File file) {
		if (file == null || !file.exists())
			throw new IllegalStateException();

		m_callback = null;
		m_writer = null;
		m_haLogFile = file;
		try {
			m_raf = new RandomAccessFile(m_haLogFile, "r");
		} catch (FileNotFoundException e) {
			// this should have been caught above and thrown
			// IllegalStateException
			throw new RuntimeException(e);
		}
		m_channel = m_raf.getChannel();

		try {
			/**
			 * Must determine whether the file has consistent open and committed
			 * rootBlocks, using the commitCounter to determine which rootBlock
			 * is which.
			 * 
			 * Note: Both root block should exist (they are both written on
			 * startup). If they are identical, then the log is empty (the
			 * closing root block has not been written and the data in the log
			 * is useless).
			 * 
			 * We figure out which root block is the opening root block based on
			 * standard logic.
			 */
			/*
			 * Read the MAGIC and VERSION.
			 */
			m_raf.seek(0L);
			try {
				/*
				 * Note: this next line will throw IOException if there is a
				 * file lock contention.
				 */
				m_magic = m_raf.readInt();
			} catch (IOException ex) {
				throw new RuntimeException(
						"Can not read magic. Is file locked by another process?",
						ex);
			}
			if (m_magic != MAGIC)
				throw new RuntimeException("Bad journal magic: expected="
						+ MAGIC + ", actual=" + m_magic);
			m_version = m_raf.readInt();
			if (m_version != VERSION1)
				throw new RuntimeException("Bad journal version: expected="
						+ VERSION1 + ", actual=" + m_version);

			final RootBlockUtility tmp = new RootBlockUtility(m_reopener, file,
					true/* validateChecksum */, false/* alternateRootBlock */,
					false/* ignoreBadRootBlock */);

			m_closeRootBlock = tmp.chooseRootBlock();

			m_openRootBlock = tmp.rootBlock0 == m_closeRootBlock ? tmp.rootBlock1
					: tmp.rootBlock0;

			final long cc0 = m_openRootBlock.getCommitCounter();

			final long cc1 = m_closeRootBlock.getCommitCounter();

			if ((cc0 + 1) != cc1 && (cc0 != cc1)) {
				/*
				 * Counters are inconsistent with either an empty log file or a
				 * single transaction scope.
				 */
				throw new IllegalStateException("Incompatible rootblocks: cc0="
						+ cc0 + ", cc1=" + cc1);
			}

			m_channel.position(START_DATA);

			m_storeType = m_openRootBlock.getStoreType();

		} catch (Throwable t) {

			try {
				close();
			} catch (IOException e) {
				log.warn(e);
			}

			throw new RuntimeException(t);

		}
	}

	/**
	 * Private method called by the HALogWriter to write a new message
	 * 
	 * @param msg
	 * @param data
	 * @throws IOException
	 */
	private void write(final IHAWriteMessage msg, final ByteBuffer data)
			throws IOException {
		m_writeLock.lock();
		try {

			/*
			 * Check if this really is a valid message for this file. If it is
			 * not, then close the file and return immediately
			 */
			if (m_openRootBlock.getCommitCounter() != msg.getCommitCounter())
				throw new IllegalStateException("commitCounter="
						+ m_openRootBlock.getCommitCounter() + ", but msg="
						+ msg);

			if (m_openRootBlock.getLastCommitTime() != msg.getLastCommitTime())
				throw new IllegalStateException("lastCommitTime="
						+ m_openRootBlock.getLastCommitTime() + ", but msg="
						+ msg);

			if (m_sequence != msg.getSequence())
				throw new IllegalStateException("nextSequence=" + m_sequence
						+ ", but msg=" + msg);

			if (log.isInfoEnabled())
				log.info("msg=" + msg + ", position=" + m_writePosition);

			if (m_writePosition < headerSize0)
				throw new AssertionError("position=" + m_writePosition
						+ ", but headerSize=" + headerSize0);

			/*
			 * Write the HAWriteMessage onto the channel.
			 */
			{
				// serialized message object (pos=0; limit=nbytes)
				final ByteBuffer tmp = bufferObject(msg);

				final int nbytes = tmp.limit();

				FileChannelUtility.writeAll(m_reopener, tmp, m_writePosition);

				m_writePosition += nbytes;

			}

			switch (m_openRootBlock.getStoreType()) {
			case RW: {
				/*
				 * Write the WriteCache block on the channel.
				 */
				final int nbytes = msg.getSize();
				assert data.position() == 0;
				assert data.limit() == nbytes;
				// Note: duplicate() to avoid side effects on ByteBuffer!!!
				FileChannelUtility.writeAll(m_reopener, data.duplicate(),
						m_writePosition);
				m_writePosition += nbytes;
				break;
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

			m_sequence++;

			m_fileChange.signalAll();

		} finally {
			m_writeLock.unlock();
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
	 * Attempts to close the file; will only succeed if there are
	 * no readers and writers.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (m_accessors == 0 && m_channel.isOpen()) {

			m_raf.close();

		}
	}

	/**
	 * Requests that the underlying log file is deleted.
	 * 
	 * @throws IOException 
	 */
	public void delete() throws IOException {
		close(); // attempt to close will succeed if there are no registered accessors
		
		if (m_channel.isOpen())
			throw new IllegalStateException("Request to delete file with open readers and writers");
		
		if (m_haLogFile.exists()) {
			try {
				m_haLogFile.delete();
			} catch (SecurityException se) {
				log.warn("unable to delete file", se);
			}
		}
	}

	/**
	 * 
	 * @return true if the file is complete and its size indicates no content
	 */
	public boolean isEmpty() {
		return (m_closeRootBlock != null)
				&& (m_openRootBlock.getCommitCounter() == m_closeRootBlock
						.getCommitCounter());
	}

	public long getCommitCounter() {
		return m_openRootBlock.getCommitCounter();
	}

	/**
	 * @return the open writer for the file if any
	 */
	public HALogWriter getWriter() {
		return m_writer != null && m_writer.isOpen() ? m_writer : null;
	}

	/**
	 * @return a new reader for the file
	 * @throws IOException
	 */
	public IHALogReader getReader() throws IOException {
		return new HALogReader();
	}

	/**
	 * Called by the HALogWriter to close the log file with the
	 * committing rootblock.
	 * 
	 * @param rbv
	 * @throws IOException
	 */
	private void close(final IRootBlockView rbv) throws IOException {
		m_writeLock.lock();
		try {
			if (m_closeRootBlock != null)
				throw new IllegalStateException("LogFile is already closed");
			
			writeRootBlock(rbv.isRootBlock0(), rbv);

			m_closeRootBlock = rbv;

			m_callback.release(this);

			m_fileChange.signalAll();
			
		} finally {
			m_writeLock.unlock();
		}
	}

	private void writeRootBlock(final boolean isRootBlock0,
			final IRootBlockView rootBlock) throws IOException {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		final long position = isRootBlock0 ? OFFSET_ROOT_BLOCK0
				: OFFSET_ROOT_BLOCK1;

		FileChannelUtility.writeAll(m_reopener, rootBlock.asReadOnlyBuffer(),
				position);

		if (log.isDebugEnabled())
			log.debug("wrote root block: " + rootBlock);

	}

	/**
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file.
	 */
	private IRootBlockView getOpeningRootBlock() {

		return m_openRootBlock;

	}

	/**
	 * The {@link IRootBlockView} for the committed state AFTER the write set
	 * contained in the HA log file.
	 * 
	 * For an open file this will be identical to the opening rootblock, it is
	 * up to the caller to determine that this file does not represent a fully
	 * committed log.
	 */

	private IRootBlockView getClosingRootBlock() {

		return m_closeRootBlock;

	}

	private IHAWriteMessage processNextBuffer(final IReadPosition position,
			final ByteBuffer clientBuffer) throws IOException {

		m_readLock.lock();
		try {

			final long startPosition = position.getPosition();
			final FileChannelInputStream chinstr = new FileChannelInputStream(
					startPosition);
			final ObjectInputStream objinstr = new ObjectInputStream(chinstr);

			final IHAWriteMessage msg;
			try {

				msg = (IHAWriteMessage) objinstr.readObject();

			} catch (ClassNotFoundException e) {

				throw new IllegalStateException(e);

			}

			long currentPosition = chinstr.getPosition();

			switch (m_storeType) {
			case RW: {

				if (clientBuffer != null
						&& msg.getSize() > clientBuffer.capacity()) {

					throw new IllegalStateException(
							"Client buffer is not large enough for logged buffer");

				}

				// Now setup client buffer to receive from the channel
				final int nbytes = msg.getSize();

				// allow null clientBuffer for IHAWriteMessage only
				if (clientBuffer != null) {
					clientBuffer.position(0);
					clientBuffer.limit(nbytes);

					// Current position on channel.

					// Robustly read of write cache block at that position into
					// the
					// caller's buffer. (pos=limit=nbytes)
					FileChannelUtility.readAll(m_reopener, clientBuffer,
							currentPosition);

					// limit=pos; pos=0;
					clientBuffer.flip(); // ready for reading

					final int chksum = new ChecksumUtility()
							.checksum(clientBuffer.duplicate());

					if (chksum != msg.getChk())
						throw new ChecksumError("Expected=" + msg.getChk()
								+ ", actual=" + chksum);

					if (clientBuffer.remaining() != nbytes)
						throw new AssertionError();
				}
				// Advance the file channel beyond the block we just read.
				currentPosition += msg.getSize();

				break;
			}
			case WORM: {
				/*
				 * Note: The WriteCache block needs to be recovered from the
				 * WORMStrategy by the caller. The clientBuffer, if supplied, is
				 * ignored and untouched.
				 * 
				 * It is permissible for the argument to be null.
				 */
				break;
			}
			default:
				throw new UnsupportedOperationException();
			}

			position.setPosition(currentPosition);

			return msg;
		} finally {
			m_readLock.unlock();
		}

	}

	/**
	 * Called from the LogManager to disable the current log file for writing.
	 * 
	 * We need not worry about concurrent access to the writer since another
	 * threads access to the writer is non-deterministic, the important state is
	 * that the calling thread would receive a null writer if it requested one.
	 * 
	 * @throws IOException
	 */
	public void disable() throws IOException {
		assert m_writer != null;

		m_writer.close();	
	}

	/**
	 * Return the local name of the HA Log file associated with the
	 * commitCounter.
	 * 
	 * @param commitCounter
	 * @return
	 */
	public static File getHALogFileName(final File dir, final long commitCounter) {

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

	private class HALogAccess {

		private volatile boolean m_open = true;

		HALogAccess() {
			m_accessors++;
		}

		public void close() throws IOException {
			if (!m_open)
				throw new IllegalStateException();

			m_open = false;

			m_accessors--;

			HALogFile.this.close();
		}

		public boolean isOpen() {
			return m_open;
		}

	}

	/**
	 * Callback interface passed to read methods allowing updating of caller
	 * after read
	 */
	interface IReadPosition {
		long getPosition();

		void setPosition(final long position);
	}

	private class HALogReader extends HALogAccess implements IHALogReader {

		public boolean isEmpty() {

			return HALogFile.this.isEmpty();

		}

		private void assertOpen() throws IOException {

			if (!isOpen())
				throw new IOException("Closed: " + HALogFile.this);

		}

//		/**
//		 * The {@link IRootBlockView} for the committed state BEFORE the write
//		 * set contained in the HA log file.
//		 */
//		public HALogFile getHALogFile() {
//
//			return HALogFile.this;
//
//		}

		public boolean hasMoreBuffers() throws IOException {

			assertOpen();

			return waitOnData(m_readPosition.getPosition());

		}

		private boolean waitOnData(final long position) throws IOException {
			m_readLock.lock();
			try {
				final long cursize = m_channel.size();

				if (position > cursize)
					throw new IllegalArgumentException();

				if (position < cursize) // data is available
					return true;

				if (m_closeRootBlock != null) // no more data will be written
					return false;
			} finally {
				m_readLock.unlock();
			}

			m_writeLock.lock();
			try {

				// Tests for new writes, closing and disabling
				while (m_closeRootBlock == null && position == m_channel.size()
						&& m_writer.isOpen()) {
					try {
						m_fileChange.await();
					} catch (InterruptedException e) {
						// ignore??
					}
				}

				// Useful to extract to final variable for DEBUG
				final long cursize = m_channel.size();

				return position < cursize;
			} finally {
				m_writeLock.unlock();
			}
		}

		private IReadPosition m_readPosition = new IReadPosition() {
			long m_position = HALogFile.START_DATA;

			@Override
			public long getPosition() {
				return m_position;
			}

			@Override
			public void setPosition(long position) {
				m_position = position;
			}

		};

		public IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer)
				throws IOException {

			return HALogFile.this.processNextBuffer(m_readPosition,
					clientBuffer);

		}

		@Override
		public IRootBlockView getClosingRootBlock() throws IOException {
			return HALogFile.this.getClosingRootBlock();
		}

		public IRootBlockView getOpeningRootBlock() {
			return HALogFile.this.getOpeningRootBlock();
		}
		
	    @Override
	    public void computeDigest(final MessageDigest digest)
	            throws DigestException, IOException {

	        HALogFile.computeDigest(m_reopener, digest);
	        
	    }
	    
	}

    static void computeDigest(final IReopenChannel<FileChannel> reopener,
            final MessageDigest digest) throws DigestException, IOException {

        IBufferAccess buf = null;
        try {

            try {
                // Acquire a buffer.
                buf = DirectBufferPool.INSTANCE.acquire();
            } catch (InterruptedException ex) {
                // Wrap and re-throw.
                throw new IOException(ex);
            }

            // The backing ByteBuffer.
            final ByteBuffer b = buf.buffer();

//            // A byte[] with the same capacity as that ByteBuffer.
//            final byte[] a = new byte[b.capacity()];

            // The capacity of that buffer (typically 1MB).
            final int bufferCapacity = b.capacity();

            // The size of the file at the moment we begin.
            final long fileExtent = reopener.reopenChannel().size();

            // The #of bytes whose digest will be computed.
            final long totalBytes = fileExtent;

            // The #of bytes remaining.
            long remaining = totalBytes;

            // The offset.
            long offset = 0L;

            // The block sequence.
            long sequence = 0L;

            if (log.isInfoEnabled())
                log.info("Computing digest: nbytes=" + totalBytes);

            while (remaining > 0) {

                final int nbytes = (int) Math.min((long) bufferCapacity,
                        remaining);

                if (log.isDebugEnabled())
                    log.debug("Computing digest: sequence=" + sequence
                            + ", offset=" + offset + ", nbytes=" + nbytes);

                // Setup for read.
                b.position(0);
                b.limit(nbytes);

                // read block
                FileChannelUtility.readAll(reopener, b, offset);

//                // Copy data into our byte[].
//                final byte[] c = BytesUtil.toArray(b, false/* forceCopy */, a);

                // update digest
//                digest.update(c, 0/* off */, nbytes/* len */);
                digest.update(b);

                offset += nbytes;
                
                remaining -= nbytes;

                sequence++;
                
            }

            if (log.isInfoEnabled())
                log.info("Computed digest: #blocks=" + sequence + ", #bytes="
                        + totalBytes);

            // Done.
            return;

        } finally {

            if (buf != null) {
                try {
                    // Release the direct buffer.
                    buf.release();
                } catch (InterruptedException e) {
                    log.warn(e);
                }
            }

        }

    }
    
    public class HALogWriter extends HALogAccess implements IHALogWriter {

		/** Current write cache block sequence counter. */
		private long m_nextSequence = 0;

		private void assertOpen() {

			if (!isOpen())
				throw new IllegalStateException();

		}

		/**
		 * Return the sequence number that is expected for the next write.
		 */
		public long getSequence() {

			assertOpen();

			return m_nextSequence;

		}

		public String toString() {

			final IRootBlockView tmp = HALogFile.this.getOpeningRootBlock();

			final long seq = m_nextSequence;

			return getClass().getName()
					+ "{"
					+ ((!isOpen()) ? "closed" : "commitCounter="
							+ tmp.getCommitCounter() + ",nextSequence=" + seq)
					+ "}";

		}

		public void write(final IHAWriteMessage msg, final ByteBuffer data)
				throws IOException {
			assertOpen();
			
			HALogFile.this.write(msg, data);
		}

		public void close(final IRootBlockView rbv) throws IOException {
			assertOpen();
			
			HALogFile.this.close(rbv);
			
			close();
		}

		public long getCommitCounter() {
			return HALogFile.this.getCommitCounter();
		}

	}
	
	/**
	 * To support streaming protocols, this simple InputStream extension
	 * provides a thread-local management of the read position, asserting the
	 * single thread assumption rather than providing a locking strategy.
	 * 
	 * It is NOT owned by a reader, it is simply created locally to facilitate a
	 * stream interface.
	 */
	private class FileChannelInputStream extends InputStream {

		final ByteBuffer m_char = ByteBuffer.allocate(1);

		// no locks are required, we will assert the thread ownership
		private long m_position;
		final Thread m_startThread = Thread.currentThread();

		FileChannelInputStream(final long position) {
			m_position = position;
		}

		@Override
		public int read() throws IOException {
			assert m_startThread == Thread.currentThread();

			m_char.position(0);
			final int status = readLocal(m_char, m_position++);
			m_char.position(0);

			// System.err.println("read() - returning: " + status);

			return (status == -1) ? -1 : m_char.get();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			assert m_startThread == Thread.currentThread();

			final ByteBuffer buffer = ByteBuffer.wrap(b, off, len);

			final int ret = readLocal(buffer, m_position);

			m_position += ret;

			// System.err.println("read(buf) - returning: " + ret +
			// ", requested: " + len);

			return ret;
		}

		/**
		 * @return the current absolute read position
		 */
		private long getPosition() {
			return m_position;
		}

		/**
		 * Simulates the normal InputStream request, returning -1 if EOF and
		 * only asking for as much as can be read - do not try to read past EOF.
		 * 
		 * In fact, we probably do not need to make these checks since the
		 * Object Streaming used only ever asks for what is expected.
		 * 
		 * @param buffer
		 * @param position
		 * @return
		 * @throws IOException
		 */
		private int readLocal(final ByteBuffer buffer, final long position)
				throws IOException {
			final int requestLen = buffer.remaining();

			final long eof = m_channel.size();
			if (eof < (position + requestLen)) {
				if (position == eof)
					return -1;

				final int len = (int) (eof - position);
				buffer.limit(len);
				FileChannelUtility.readAll(m_reopener, buffer, position);
				buffer.limit(requestLen);
			} else {
				FileChannelUtility.readAll(m_reopener, buffer, position);
			}

			return requestLen - buffer.remaining();
		}

	}

	/*
	 * Used for unit tests
	 */
	public boolean isOpen() {
		return m_channel != null && m_channel.isOpen();
	}

	/**
	 * 
	 * @return the File object for this HALogFile
	 */
	public File getFile() {
		return m_haLogFile;
	}

}
