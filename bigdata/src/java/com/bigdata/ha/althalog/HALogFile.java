package com.bigdata.ha.althalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.log.Log;

import com.bigdata.ha.althalog.HALogManager.IHALogManagerCallback;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

/**
 * 
 * @author Martyn Cutcher
 * 
 */
public class HALogFile {

	public static final String HA_LOG_EXT = ".ha-log";

	/**
	 * Logger for HA events.
	 */
	private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

	/**
	 * Ensures private communication from writer/reader(s)
	 */
	interface IHALogFileCallback {
		HALogFile getHALogFile();

		void close() throws IOException;

		boolean isOpen();
	}

	private volatile int m_accessors = 0;

	private class HALogFileCallback implements IHALogFileCallback {

		private volatile boolean m_open = true;

		// could really be a private synchronized object
		private final ReentrantLock m_simpleLock = new ReentrantLock();

		HALogFileCallback() {
			m_accessors++;
		}

		@Override
		public void close() throws IOException {
			// protect against multiple/concurrent close
			m_simpleLock.lock();
			try {
				if (!m_open)
					throw new IllegalStateException();

				m_open = false;

				m_accessors--;

				HALogFile.this.close();
			} finally {
				m_simpleLock.unlock();
			}
		}

		@Override
		public boolean isOpen() {
			return m_open;
		}

		@Override
		public HALogFile getHALogFile() {
			return HALogFile.this;
		}

		public long getEOF() throws IOException {
			return m_channel.size();
		}
	}

	interface IHALogFileReaderCallback extends IHALogFileCallback {
		boolean waitOnData(long position) throws IOException;

		IHAWriteMessage processNextBuffer(IReadPosition position,
				ByteBuffer clientBuffer) throws IOException;

		IRootBlockView getClosingRootBlock();

		IRootBlockView getOpeningRootBlock();
	}

	private class HALogFileReaderCallback extends HALogFileCallback implements
			IHALogFileReaderCallback {
		@Override
		public boolean waitOnData(final long position) throws IOException {
			m_fileChangeSignal.lock();
			try {
				if (position > m_channel.size())
					throw new IllegalArgumentException();

				while (m_closeRootBlock == null && position == m_channel.size()) {
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
				m_fileChangeSignal.unlock();
			}
		}

		@Override
		public IHAWriteMessage processNextBuffer(final IReadPosition position,
				final ByteBuffer clientBuffer) throws IOException {

			return HALogFile.this.processNextBuffer(position, clientBuffer);
		}

		@Override
		public IRootBlockView getClosingRootBlock() {
			return HALogFile.this.getClosingRootBlock();
		}

		@Override
		public IRootBlockView getOpeningRootBlock() {
			return HALogFile.this.getOpeningRootBlock();
		}

	}

	interface IHALogFileWriterCallback extends IHALogFileCallback {
		void write(final IHAWriteMessage msg, final ByteBuffer data)
				throws IOException;

		void close(IRootBlockView rbv) throws IOException;
	}

	private class HALogFileWriterCallback extends HALogFileCallback implements
			IHALogFileWriterCallback {

		@Override
		public void write(final IHAWriteMessage msg, final ByteBuffer data)
				throws IOException {
			if (isOpen()) {
				HALogFile.this.write(msg, data);
			} else {
				throw new IllegalStateException();
			}
		}

		@Override
		public void close(IRootBlockView rbv) throws IOException {
			if (isOpen()) {
				HALogFile.this.close(rbv);
				close();
			} else {
				throw new IllegalStateException();
			}
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

	private IHALogManagerCallback m_callback;

	private final IRootBlockView m_openRootBlock;
	private IRootBlockView m_closeRootBlock;

	private final StoreTypeEnum m_storeType;
	private final File m_haLogFile;
	private final FileChannel m_channel;
	private final RandomAccessFile m_raf;

	private final int m_magic;
	private final int m_version;

	private int m_sequence = 0;

	final IReopenChannel<FileChannel> m_reopener = new IReopenChannel<FileChannel>() {

		@Override
		public FileChannel reopenChannel() throws IOException {

			if (m_channel == null)
				throw new IOException("Closed");

			return m_channel;

		}
	};

	private HALogWriter m_writer;

	/**
	 * We need to take a lock whenever we are updating the file and signal the
	 * fileChange condition to alert any waiting readers.
	 */
	// private final ReadWriteLock m_lock = new ReentrantReadWriteLock();
	// private final Lock m_writeLock = m_lock.writeLock();
	// private final Lock m_readLock = m_lock.readLock();
	// Do not want concurrent reading and writing, so single lock is required
	private final Lock m_lock = new ReentrantLock();
	private final Lock m_writeLock = m_lock;
	private final Lock m_readLock = m_lock;

	private final Lock m_fileChangeSignal = new ReentrantLock();
	private final Condition m_fileChange = m_fileChangeSignal.newCondition();

	private void wakeUpReaders() {
		// wake up any waiting readers
		m_fileChangeSignal.lock();
		try {
			m_fileChange.signalAll();
		} finally {
			m_fileChangeSignal.unlock();
		}

	}

	private long m_writePosition = -1;

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
		final File hadir = m_callback.getHALogDir();
		m_haLogFile = new File(hadir, getHALogFileName(rbv.getCommitCounter())
				+ HA_LOG_EXT);
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
		writeRootBlock(true, rbv);
		writeRootBlock(false, rbv);

		m_writePosition = START_DATA;

		m_writer = new HALogWriter(new HALogFileWriterCallback());
		
		if (haLog.isInfoEnabled())
			haLog.info("Opening HALogFile: " + m_haLogFile.getAbsolutePath());

	}

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

			if (haLog.isInfoEnabled())
				haLog.info("msg=" + msg + ", position=" + m_writePosition);

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

		} finally {
			m_writeLock.unlock();
		}

		wakeUpReaders();
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
	 * This constructor creates a read only view of the log file
	 * 
	 * @param readonlyLog
	 * @throws FileNotFoundException
	 */
	public HALogFile(final File file) {
		if (file == null || !file.exists())
			throw new IllegalStateException();

		m_callback = null;
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
				haLog.warn(e);
			}

			throw new RuntimeException(t);

		}
	}

	private void close() throws IOException {
		if (m_accessors == 0 && m_channel.isOpen()) {

				m_raf.close();

		}
	}

	public void delete() {
		// TODO Auto-generated method stub

	}

	private void close(final IRootBlockView rbv) throws IOException {
		m_writeLock.lock();
		try {
			writeRootBlock(rbv.isRootBlock0(), rbv);

			m_closeRootBlock = rbv;

			m_callback.release(this);

		} finally {
			m_writeLock.unlock();
		}

		wakeUpReaders();
	}

	/**
	 * 
	 * @return a new reader
	 * @throws IOException
	 */
	public HALogReader getReader() throws IOException {
		return new HALogReader(new HALogFileReaderCallback());
	}

	/**
	 * 
	 * @return the writer for the new file if any
	 */
	public HALogWriter getWriter() {
		return m_writer;
	}

	private void writeRootBlock(final boolean isRootBlock0,
			final IRootBlockView rootBlock) throws IOException {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		final long position = isRootBlock0 ? OFFSET_ROOT_BLOCK0
				: OFFSET_ROOT_BLOCK1;

		FileChannelUtility.writeAll(m_reopener, rootBlock.asReadOnlyBuffer(),
				position);

		if (haLog.isDebugEnabled())
			haLog.debug("wrote root block: " + rootBlock);

	}

	/**
	 * The {@link IRootBlockView} for the committed state BEFORE the write set
	 * contained in the HA log file.
	 */
	private IRootBlockView getOpeningRootBlock() {

		return m_openRootBlock;

	}

	private IRootBlockView getClosingRootBlock() {

		return m_closeRootBlock;

	}

	/**
	 * Return the local name of the HA Log file associated with the
	 * 
	 * @param commitCounter
	 * @return
	 */
	public static String getHALogFileName(final long commitCounter) {

		/*
		 * Format the name of the log file.
		 * 
		 * Note: The commit counter in the file name should be zero filled to 20
		 * digits so we have the files in lexical order in the file system (for
		 * convenience).
		 */
		final String logFile;
		{

			final StringBuilder sb = new StringBuilder();

			final Formatter f = new Formatter(sb);

			f.format("%020d" + HA_LOG_EXT, commitCounter);
			f.flush();
			f.close();

			logFile = sb.toString();

		}

		return logFile;

	}

	public boolean isEmpty() {
		return (m_closeRootBlock != null)
				&& (m_openRootBlock.getCommitCounter() == m_closeRootBlock
						.getCommitCounter());
	}

	public long getCommitCounter() {
		return m_openRootBlock.getCommitCounter();
	}

	/**
	 * To stream from the Channel, we can use the associated RandomAccessFile
	 * since the FilePointer for one is the same as the other.
	 */
	private static class RAFInputStream extends InputStream {
		final RandomAccessFile m_raf;

		RAFInputStream(final RandomAccessFile raf) {
			m_raf = raf;
		}

		@Override
		public int read() throws IOException {
			return m_raf.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return m_raf.read(b, off, len);
		}

	}

	private IHAWriteMessage processNextBuffer(final IReadPosition position,
			final ByteBuffer clientBuffer) throws IOException {

		m_readLock.lock();
		try {
			final FileChannel channel = m_raf.getChannel();
			long readpos = position.getPosition();
			channel.position(readpos);

			final ObjectInputStream objinstr = new ObjectInputStream(
					new RAFInputStream(m_raf));

			final IHAWriteMessage msg;
			try {

				msg = (IHAWriteMessage) objinstr.readObject();

			} catch (ClassNotFoundException e) {

				throw new IllegalStateException(e);

			}

			switch (m_storeType) {
			case RW: {

				if (msg.getSize() > clientBuffer.capacity()) {

					throw new IllegalStateException(
							"Client buffer is not large enough for logged buffer");

				}

				// Now setup client buffer to receive from the channel
				final int nbytes = msg.getSize();
				clientBuffer.position(0);
				clientBuffer.limit(nbytes);

				// Current position on channel.
				readpos = channel.position();

				// allow null clientBuffer for IHAWriteMessage only
				if (clientBuffer != null) {
					// Robustly read of write cache block at that position into
					// the
					// caller's buffer. (pos=limit=nbytes)
					FileChannelUtility.readAll(m_reopener, clientBuffer,
							readpos);

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
				readpos += msg.getSize();

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

			position.setPosition(readpos);

			return msg;
		} finally {
			m_readLock.unlock();
		}

	}

	/**
	 * Called from the LogManager to disable the current log file for writing.
	 * 
	 * We need not worry about concurrent access to the writer since another threads
	 * access to the writer is non-deterministic, the important state is that the calling
	 * thread would receive a null writer if it requested one.
	 * @throws IOException 
	 */
	public void disable() throws IOException {
		assert m_writer != null;

		m_writer.close();
		
		m_writer = null;
	}
}
